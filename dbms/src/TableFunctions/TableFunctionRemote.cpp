#include <DB/TableFunctions/getStructureOfRemoteTable.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Interpreters/evaluateConstantExpression.h>
#include <DB/Interpreters/Cluster.h>

#include <DB/TableFunctions/TableFunctionRemote.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
	extern const int BAD_ARGUMENTS;
}


/// Декартово произведение двух множеств строк, результат записываем на место первого аргумента
static void append(std::vector<String> & to, const std::vector<String> & what, size_t max_addresses)
{
	if (what.empty())
		return;

	if (to.empty())
	{
		to = what;
		return;
	}

	if (what.size() * to.size() > max_addresses)
		throw Exception("Storage Distributed, first argument generates too many result addresses",
						ErrorCodes::BAD_ARGUMENTS);
	std::vector<String> res;
	for (size_t i = 0; i < to.size(); ++i)
		for (size_t j = 0; j < what.size(); ++j)
			res.push_back(to[i] + what[j]);

	to.swap(res);
}


/// Парсим число из подстроки
static bool parseNumber(const String & description, size_t l, size_t r, size_t & res)
{
	res = 0;
	for (size_t pos = l; pos < r; pos ++)
	{
		if (!isdigit(description[pos]))
			return false;
		res = res * 10 + description[pos] - '0';
		if (res > 1e15)
			return false;
	}
	return true;
}


/* Парсит строку, генерирующую шарды и реплики. Разделитель - один из двух символов | или ,
	* в зависимости от того генерируются шарды или реплики.
	* Например:
	* host1,host2,... - порождает множество шардов из host1, host2, ...
	* host1|host2|... - порождает множество реплик из host1, host2, ...
	* abc{8..10}def - порождает множество шардов abc8def, abc9def, abc10def.
	* abc{08..10}def - порождает множество шардов abc08def, abc09def, abc10def.
	* abc{x,yy,z}def - порождает множество шардов abcxdef, abcyydef, abczdef.
	* abc{x|yy|z}def - порождает множество реплик abcxdef, abcyydef, abczdef.
	* abc{1..9}de{f,g,h} - прямое произведение, 27 шардов.
	* abc{1..9}de{0|1} - прямое произведение, 9 шардов, в каждом 2 реплики.
	*/
static std::vector<String> parseDescription(const String & description, size_t l, size_t r, char separator, size_t max_addresses)
{
	std::vector<String> res;
	std::vector<String> cur;

	/// Пустая подстрока, означает множество из пустой строки
	if (l >= r)
	{
		res.push_back("");
		return res;
	}

	for (size_t i = l; i < r; ++i)
	{
		/// Либо числовой интервал (8..10) либо аналогичное выражение в скобках
		if (description[i] == '{')
		{
			int cnt = 1;
			int last_dot = -1; /// Самая правая пара точек, запоминаем индекс правой из двух
			size_t m;
			std::vector<String> buffer;
			bool have_splitter = false;

			/// Ищем соответствующую нашей закрывающую скобку
			for (m = i + 1; m < r; ++m)
			{
				if (description[m] == '{') ++cnt;
				if (description[m] == '}') --cnt;
				if (description[m] == '.' && description[m-1] == '.') last_dot = m;
				if (description[m] == separator) have_splitter = true;
				if (cnt == 0) break;
			}
			if (cnt != 0)
				throw Exception("Storage Distributed, incorrect brace sequence in first argument",
								ErrorCodes::BAD_ARGUMENTS);
			/// Наличие точки означает, что числовой интервал
			if (last_dot != -1)
			{
				size_t left, right;
				if (description[last_dot - 1] != '.')
					throw Exception("Storage Distributed, incorrect argument in braces (only one dot): " + description.substr(i, m - i + 1),
									ErrorCodes::BAD_ARGUMENTS);
				if (!parseNumber(description, i + 1, last_dot - 1, left))
					throw Exception("Storage Distributed, incorrect argument in braces (Incorrect left number): "
									+ description.substr(i, m - i + 1),
									ErrorCodes::BAD_ARGUMENTS);
				if (!parseNumber(description, last_dot + 1, m, right))
					throw Exception("Storage Distributed, incorrect argument in braces (Incorrect right number): "
									+ description.substr(i, m - i + 1),
									ErrorCodes::BAD_ARGUMENTS);
				if (left > right)
					throw Exception("Storage Distributed, incorrect argument in braces (left number is greater then right): "
									+ description.substr(i, m - i + 1),
									ErrorCodes::BAD_ARGUMENTS);
				if (right - left + 1 >  max_addresses)
					throw Exception("Storage Distributed, first argument generates too many result addresses",
						ErrorCodes::BAD_ARGUMENTS);
				bool add_leading_zeroes = false;
				size_t len = last_dot - 1 - (i + 1);
				/// Если у левой и правой границы поровну цифр, значит необходимо дополнять лидирующими нулями.
				if (last_dot - 1 - (i + 1) == m - (last_dot + 1))
					add_leading_zeroes = true;
				for (size_t id = left; id <= right; ++id)
				{
					String cur = toString<uint64>(id);
					if (add_leading_zeroes)
					{
						while (cur.size() < len)
							cur = "0" + cur;
					}
					buffer.push_back(cur);
				}
			} else if (have_splitter) /// Если внутри есть текущий разделитель, то сгенерировать множество получаемых строк
				buffer = parseDescription(description, i + 1, m, separator, max_addresses);
			else 					/// Иначе просто скопировать, порождение произойдет при вызове с правильным разделителем
				buffer.push_back(description.substr(i, m - i + 1));
			/// К текущему множеству строк добавить все возможные полученные продолжения
			append(cur, buffer, max_addresses);
			i = m;
		}
		else if (description[i] == separator)
		{
			/// Если разделитель, то добавляем в ответ найденные строки
			res.insert(res.end(), cur.begin(), cur.end());
			cur.clear();
		}
		else
		{
			/// Иначе просто дописываем символ к текущим строкам
			std::vector<String> buffer;
			buffer.push_back(description.substr(i, 1));
			append(cur, buffer, max_addresses);
		}
	}
	res.insert(res.end(), cur.begin(), cur.end());
	if (res.size() > max_addresses)
		throw Exception("Storage Distributed, first argument generates too many result addresses",
						ErrorCodes::BAD_ARGUMENTS);
	return res;
}


StoragePtr TableFunctionRemote::execute(ASTPtr ast_function, Context & context) const
{
	ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

	const char * err = "Table function 'remote' requires from 2 to 5 parameters: "
		"addresses pattern, name of remote database, name of remote table, [username, [password]].";

	if (args_func.size() != 1)
		throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

	if (args.size() < 2 || args.size() > 5)
		throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	String description;
	String remote_database;
	String remote_table;
	String username;
	String password;

	size_t arg_num = 0;

	auto getStringLiteral = [](const IAST & node, const char * description)
	{
		const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(&node);
		if (!lit)
			throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

		if (lit->value.getType() != Field::Types::String)
			throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

		return safeGet<const String &>(lit->value);
	};

	description = getStringLiteral(*args[arg_num], "Hosts pattern");
	++arg_num;

	args[arg_num] = evaluateConstantExpressionOrIdentidierAsLiteral(args[arg_num], context);
	remote_database = static_cast<const ASTLiteral &>(*args[arg_num]).value.safeGet<String>();
	++arg_num;

	size_t dot = remote_database.find('.');
	if (dot != String::npos)
	{
		/// NOTE Плохо - не поддерживаются идентификаторы в обратных кавычках.
		remote_table = remote_database.substr(dot + 1);
		remote_database = remote_database.substr(0, dot);
	}
	else
	{
		if (arg_num >= args.size())
			throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		args[arg_num] = evaluateConstantExpressionOrIdentidierAsLiteral(args[arg_num], context);
		remote_table = static_cast<const ASTLiteral &>(*args[arg_num]).value.safeGet<String>();
		++arg_num;
	}

	if (arg_num < args.size())
	{
		username = getStringLiteral(*args[arg_num], "Username");
		++arg_num;
	}
	else
		username = "default";

	if (arg_num < args.size())
	{
		password = getStringLiteral(*args[arg_num], "Password");
		++arg_num;
	}

	if (arg_num < args.size())
		throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	/// В InterpreterSelectQuery будет создан ExpressionAnalzyer, который при обработке запроса наткнется на эти Identifier.
	/// Нам необходимо их пометить как имя базы данных или таблицы, поскольку по умолчанию стоит значение column.
	for (auto & arg : args)
		if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(arg.get()))
			id->kind = ASTIdentifier::Table;

	size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;

	std::vector<std::vector<String>> names;
	std::vector<String> shards = parseDescription(description, 0, description.size(), ',', max_addresses);

	for (size_t i = 0; i < shards.size(); ++i)
		names.push_back(parseDescription(shards[i], 0, shards[i].size(), '|', max_addresses));

	if (names.empty())
		throw Exception("Shard list is empty after parsing first argument", ErrorCodes::BAD_ARGUMENTS);

	auto cluster = std::make_shared<Cluster>(context.getSettings(), names, username, password);

	return StorageDistributed::create(
		getName(),
		std::make_shared<NamesAndTypesList>(getStructureOfRemoteTable(*cluster, remote_database, remote_table, context)),
		remote_database,
		remote_table,
		cluster,
		context);
}

}
