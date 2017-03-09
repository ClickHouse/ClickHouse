#include <DB/Columns/ColumnString.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Storages/System/StorageSystemZooKeeper.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Interpreters/Context.h>
#include <zkutil/ZooKeeper.h>


namespace DB
{


StorageSystemZooKeeper::StorageSystemZooKeeper(const std::string & name_)
	: name(name_)
	, columns{
		{ "name", 			std::make_shared<DataTypeString>()	},
		{ "value", 			std::make_shared<DataTypeString>()	},
		{ "czxid",			std::make_shared<DataTypeInt64>()	},
		{ "mzxid",			std::make_shared<DataTypeInt64>()	},
		{ "ctime",			std::make_shared<DataTypeDateTime>()},
		{ "mtime",			std::make_shared<DataTypeDateTime>()},
		{ "version",		std::make_shared<DataTypeInt32>()	},
		{ "cversion",		std::make_shared<DataTypeInt32>()	},
		{ "aversion",		std::make_shared<DataTypeInt32>()	},
		{ "ephemeralOwner",	std::make_shared<DataTypeInt64>()	},
		{ "dataLength",		std::make_shared<DataTypeInt32>()	},
		{ "numChildren",	std::make_shared<DataTypeInt32>()	},
		{ "pzxid",			std::make_shared<DataTypeInt64>()	},
		{ "path", 			std::make_shared<DataTypeString>()	},
	}
{
}

StoragePtr StorageSystemZooKeeper::create(const std::string & name_)
{
	return make_shared(name_);
}


static bool extractPathImpl(const IAST & elem, String & res)
{
	const ASTFunction * function = typeid_cast<const ASTFunction *>(&elem);
	if (!function)
		return false;

	if (function->name == "and")
	{
		for (size_t i = 0; i < function->arguments->children.size(); ++i)
			if (extractPathImpl(*function->arguments->children[i], res))
				return true;

		return false;
	}

	if (function->name == "equals")
	{
		const ASTExpressionList & args = typeid_cast<const ASTExpressionList &>(*function->arguments);
		const IAST * value;

		if (args.children.size() != 2)
			return false;

		const ASTIdentifier * ident;
		if ((ident = typeid_cast<const ASTIdentifier *>(&*args.children.at(0))))
			value = &*args.children.at(1);
		else if ((ident = typeid_cast<const ASTIdentifier *>(&*args.children.at(1))))
			value = &*args.children.at(0);
		else
			return false;

		if (ident->name != "path")
			return false;

		const ASTLiteral * literal = typeid_cast<const ASTLiteral *>(value);
		if (!literal)
			return false;

		if (literal->value.getType() != Field::Types::String)
			return false;

		res = literal->value.safeGet<String>();
		return true;
	}

	return false;
}


/** Вынимает из запроса условие вида path = 'path', из конъюнкций в секции WHERE.
  */
static String extractPath(const ASTPtr & query)
{
	const ASTSelectQuery & select = typeid_cast<const ASTSelectQuery &>(*query);
	if (!select.where_expression)
		return "";

	String res;
	return extractPathImpl(*select.where_expression, res) ? res : "";
}


BlockInputStreams StorageSystemZooKeeper::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	String path = extractPath(query);
	if (path.empty())
		throw Exception("SELECT from system.zookeeper table must contain condition like path = 'path' in WHERE clause.");

	ColumnWithTypeAndName col_name			{ std::make_shared<ColumnString>(),	std::make_shared<DataTypeString>(),	"name" };
	ColumnWithTypeAndName col_value			{ std::make_shared<ColumnString>(),	std::make_shared<DataTypeString>(),	"value" };
	ColumnWithTypeAndName col_czxid			{ std::make_shared<ColumnInt64>(),	std::make_shared<DataTypeInt64>(),	"czxid" };
	ColumnWithTypeAndName col_mzxid			{ std::make_shared<ColumnInt64>(),	std::make_shared<DataTypeInt64>(),	"mzxid" };
	ColumnWithTypeAndName col_ctime			{ std::make_shared<ColumnUInt32>(),	std::make_shared<DataTypeDateTime>(), "ctime" };
	ColumnWithTypeAndName col_mtime			{ std::make_shared<ColumnUInt32>(),	std::make_shared<DataTypeDateTime>(), "mtime" };
	ColumnWithTypeAndName col_version		{ std::make_shared<ColumnInt32>(),	std::make_shared<DataTypeInt32>(),	"version" };
	ColumnWithTypeAndName col_cversion		{ std::make_shared<ColumnInt32>(),	std::make_shared<DataTypeInt32>(),	"cversion" };
	ColumnWithTypeAndName col_aversion		{ std::make_shared<ColumnInt32>(),	std::make_shared<DataTypeInt32>(),	"aversion" };
	ColumnWithTypeAndName col_ephemeralOwner{ std::make_shared<ColumnInt64>(),	std::make_shared<DataTypeInt64>(),	"ephemeralOwner" };
	ColumnWithTypeAndName col_dataLength	{ std::make_shared<ColumnInt32>(),	std::make_shared<DataTypeInt32>(),	"dataLength" };
	ColumnWithTypeAndName col_numChildren	{ std::make_shared<ColumnInt32>(),	std::make_shared<DataTypeInt32>(),	"numChildren" };
	ColumnWithTypeAndName col_pzxid			{ std::make_shared<ColumnInt64>(),	std::make_shared<DataTypeInt64>(),	"pzxid" };
	ColumnWithTypeAndName col_path			{ std::make_shared<ColumnString>(),	std::make_shared<DataTypeString>(),	"path" };

	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

	/// Во всех случаях кроме корня, path не должен заканчиваться на слеш.
	String path_corrected = path;
	if (path_corrected != "/" && path_corrected.back() == '/')
		path_corrected.resize(path_corrected.size() - 1);

	zkutil::Strings nodes = zookeeper->getChildren(path_corrected);

	String path_part = path_corrected;
	if (path_part == "/")
		path_part.clear();

	std::vector<zkutil::ZooKeeper::TryGetFuture> futures;
	futures.reserve(nodes.size());
	for (const String & node : nodes)
		futures.push_back(zookeeper->asyncTryGet(path_part + '/' + node));

	for (size_t i = 0, size = nodes.size(); i < size; ++i)
	{
		auto res = futures[i].get();
		if (!res.exists)
			continue;	/// Ноду успели удалить.

		const zkutil::Stat & stat = res.stat;

		col_name.column->insert(nodes[i]);
		col_value.column->insert(res.value);
		col_czxid.column->insert(Int64(stat.czxid));
		col_mzxid.column->insert(Int64(stat.mzxid));
		col_ctime.column->insert(UInt64(stat.ctime / 1000));
		col_mtime.column->insert(UInt64(stat.mtime / 1000));
		col_version.column->insert(Int64(stat.version));
		col_cversion.column->insert(Int64(stat.cversion));
		col_aversion.column->insert(Int64(stat.aversion));
		col_ephemeralOwner.column->insert(Int64(stat.ephemeralOwner));
		col_dataLength.column->insert(Int64(stat.dataLength));
		col_numChildren.column->insert(Int64(stat.numChildren));
		col_pzxid.column->insert(Int64(stat.pzxid));
		col_path.column->insert(path);			/// Здесь именно оригинальный path. Чтобы при обработке запроса, условие в WHERE срабатывало.
	}

	Block block{
		col_name,
		col_value,
		col_czxid,
		col_mzxid,
		col_ctime,
		col_mtime,
		col_version,
		col_cversion,
		col_aversion,
		col_ephemeralOwner,
		col_dataLength,
		col_numChildren,
		col_pzxid,
		col_path,
	};

	return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(block));
}


}
