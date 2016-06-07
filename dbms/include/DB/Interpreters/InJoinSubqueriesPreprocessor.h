#pragma once

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTJoin.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Storages/IStorage.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/Interpreters/Context.h>

#include <deque>
#include <unordered_map>
#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
	extern const int DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED;
}

namespace
{

using NameToAttributes = std::unordered_map<std::string, IAST::Attributes>;

const NameToAttributes name_to_attributes =
{
	{ "in", IAST::IsIn },
	{ "notIn", IAST::IsNotIn },
	{ "globalIn", IAST::IsIn | IAST::IsGlobal },
	{ "globalNotIn", IAST::IsNotIn | IAST::IsGlobal }
};

/// Из названия секции IN получить соответствующие атрибуты.
IAST::Attributes getAttributesFromInSubqueryName(const std::string & name)
{
	auto it = name_to_attributes.find(name);
	if (it != name_to_attributes.end())
		return it->second;
	else
		return 0;
}

/// Из атрибутов составить название секции IN.
std::string getNameFromInSubqueryAttributes(IAST::Attributes attributes)
{
	std::string name;

	if (attributes & IAST::IsIn)
	{
		if (attributes & IAST::IsGlobal)
			name = "globalIn";
		else
			name = "in";
	}
	else if (attributes & IAST::IsNotIn)
	{
		if (attributes & IAST::IsGlobal)
			name = "globalNotIn";
		else
			name = "notIn";
	}

	return name;
}

/// Проверить, указана ли таблица в секции FROM.
bool isQueryFromTable(const ASTSelectQuery & query)
{
	if (query.table)
	{
		if (typeid_cast<const ASTSelectQuery *>(query.table.get()) != nullptr)
			return false;
		else if (typeid_cast<const ASTFunction *>(query.table.get()) != nullptr)
			return false;
		else
			return true;
	}
	return false;
}

/// Проверить, является ли движок распределённым с количеством шардов более одного.
template <typename TStorageDistributed>
bool isEligibleStorageForInJoinPreprocessing(const StoragePtr & storage)
{
	if (!storage)
		return false;
	if (!storage->isRemote())
		return false;

	auto storage_distributed = static_cast<TStorageDistributed *>(storage.get());
	if (storage_distributed->getShardCount() < 2)
		return false;

	return true;
}

}

/** Этот класс предоставляет контроль над выполнением распределённых запросов внутри секций IN или JOIN.
  * Мы используем шаблон, потому что движок StorageDistributed слишком сложный, чтобы писать юнит-тесты,
  * которые бы зависели от него.
  */
template <typename TStorageDistributed = StorageDistributed, typename Enable = void>
class InJoinSubqueriesPreprocessor;

template <typename TStorageDistributed>
class InJoinSubqueriesPreprocessor<TStorageDistributed,
	typename std::enable_if<std::is_base_of<IStorage, TStorageDistributed>::value>::type> final
{
public:
	InJoinSubqueriesPreprocessor(ASTSelectQuery * select_query_,
		const Context & context_, const StoragePtr & storage_)
		: select_query(select_query_), context(context_), settings(context.getSettingsRef()), storage(storage_)
	{
	}

	InJoinSubqueriesPreprocessor(const InJoinSubqueriesPreprocessor &) = delete;
	InJoinSubqueriesPreprocessor & operator=(const InJoinSubqueriesPreprocessor &) = delete;

	/** В зависимости от профиля пользователя проверить наличие прав на выполнение
	  * распределённых подзапросов внутри секций IN или JOIN и обработать эти подзапросы.
	  */
	void perform()
	{
		if (settings.distributed_product_mode == DistributedProductMode::ALLOW)
		{
			/// Согласно профиля пользователя распределённые подзапросы внутри секций IN и JOIN разрешены.
			/// Ничего не делаем.
			return;
		}

		if (select_query == nullptr)
			return;

		/// Проверить главный запрос. В секции FROM должна быть указана распределённая таблица
		/// с количеством шардов более одного. Табличные функции пропускаем.

		if (select_query->attributes & IAST::IsPreprocessedForInJoinSubqueries)
			return;

		if (!isQueryFromTable(*select_query))
		{
			select_query->setAttributes(IAST::IsPreprocessedForInJoinSubqueries);
			return;
		}

		if (!isEligibleStorageForInJoinPreprocessing<TStorageDistributed>(storage))
		{
			select_query->setAttributes(IAST::IsPreprocessedForInJoinSubqueries);
			return;
		}

		/// Собрать информацию про все подзапросы внутри секций IN или JOIN.
		/// Обработать те подзапросы, которые распределённые.

		std::deque<IAST *> to_preprocess;
		to_preprocess.push_back(select_query);

		while (!to_preprocess.empty())
		{
			auto node = to_preprocess.back();
			to_preprocess.pop_back();

			ASTFunction * function;
			ASTJoin * join;
			ASTSelectQuery * sub_select_query;

			if ((function = typeid_cast<ASTFunction *>(node)) != nullptr)
			{
				auto attributes = getAttributesFromInSubqueryName(function->name);
				if (attributes != 0)
				{
					/// Найдена секция IN.
					node->enclosing_in_or_join = node;
					node->attributes |= attributes;
				}
			}
			else if ((join = typeid_cast<ASTJoin *>(node)) != nullptr)
			{
				/// Найдена секция JOIN.
				node->enclosing_in_or_join = node;
				node->attributes |= IAST::IsJoin;
				if (join->locality == ASTJoin::Global)
					node->attributes |= IAST::IsGlobal;
			}
			else if ((node != static_cast<IAST *>(select_query))
				&& ((sub_select_query = typeid_cast<ASTSelectQuery *>(node)) != nullptr))
			{
				++node->select_query_depth;

				if (sub_select_query->enclosing_in_or_join != nullptr)
				{
					/// Найден подзапрос внутри секции IN или JOIN.
					preprocessSubquery(*sub_select_query);
				}
			}

			if (!(node->attributes & IAST::IsPreprocessedForInJoinSubqueries))
			{
				for (auto & child : node->children)
				{
					if (!(child->attributes & IAST::IsPreprocessedForInJoinSubqueries))
					{
						auto n = child.get();
						n->enclosing_in_or_join = node->enclosing_in_or_join;
						n->select_query_depth = node->select_query_depth;
						to_preprocess.push_back(n);
					}
				}

				node->attributes |= IAST::IsPreprocessedForInJoinSubqueries;
			}
		}
	}

private:
	void preprocessSubquery(ASTSelectQuery & sub_select_query)
	{
		auto & enclosing_in_or_join = *sub_select_query.enclosing_in_or_join;
		bool is_global = enclosing_in_or_join.attributes & IAST::IsGlobal;

		/// Если подзапрос внутри секции IN или JOIN является непосредственным потомком
		/// главного запроса и указано ключевое слово GLOBAL, то подзапрос пропускается.
		if ((sub_select_query.select_query_depth == 1) && is_global)
		{
			sub_select_query.attributes |= IAST::IsPreprocessedForInJoinSubqueries;
			return;
		}

		auto subquery_table_storage = getDistributedSubqueryStorage(sub_select_query);
		if (!subquery_table_storage)
			return;

		if (settings.distributed_product_mode == DistributedProductMode::DENY)
		{
			/// Согласно профиля пользователя распределённые подзапросы внутри секций IN и JOIN запрещены.
			throw Exception("Double-distributed IN/JOIN subqueries is denied (distributed_product_mode = 'deny')."
				" You may rewrite query to use local tables in subqueries, or use GLOBAL keyword, or set distributed_product_mode to suitable value.",
				ErrorCodes::DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED);
		}
		else if (settings.distributed_product_mode == DistributedProductMode::GLOBAL)
		{
			/// Согласно профиля пользователя распределённые подзапросы внутри секций IN и JOIN разрешены.
			/// Преобразовать [NOT] IN в GLOBAL [NOT] IN, и JOIN в GLOBAL JOIN.

			if (!is_global)
			{
				if (enclosing_in_or_join.attributes & IAST::IsJoin)
				{
					auto & join = static_cast<ASTJoin &>(enclosing_in_or_join);
					join.locality = ASTJoin::Global;
				}
				else if (enclosing_in_or_join.attributes & (IAST::IsIn | IAST::IsNotIn))
				{
					auto & function = static_cast<ASTFunction &>(enclosing_in_or_join);
					function.name = getNameFromInSubqueryAttributes(function.attributes | IAST::IsGlobal);
				}
				else
					throw Exception("InJoinSubqueriesPreprocessor: Internal error", ErrorCodes::LOGICAL_ERROR);
			}
		}
		else if (settings.distributed_product_mode == DistributedProductMode::LOCAL)
		{
			/// Согласно профиля пользователя распределённые подзапросы внутри секций IN и JOIN разрешены.
			/// Преобразовать распределённую таблицу в соответствующую удалённую таблицу.

			auto & distributed_storage = static_cast<TStorageDistributed &>(*subquery_table_storage);

			if (sub_select_query.database.isNull())
			{
				sub_select_query.database = new ASTIdentifier{{}, distributed_storage.getRemoteDatabaseName(),
					ASTIdentifier::Database};

				/// Поскольку был создан новый узел для БД, необходимо его вставить в список
				/// потомков этого подзапроса. См. ParserSelectQuery для структуры потомков.
				if (sub_select_query.children.size() < 2)
					throw Exception("InJoinSubqueriesPreprocessor: Internal error", ErrorCodes::LOGICAL_ERROR);
				auto it = ++sub_select_query.children.begin();
				sub_select_query.children.insert(it, sub_select_query.database);
			}
			else
			{
				auto & db_name = typeid_cast<ASTIdentifier &>(*sub_select_query.database).name;
				db_name = distributed_storage.getRemoteDatabaseName();
			}

			auto & table_name = typeid_cast<ASTIdentifier &>(*sub_select_query.table).name;
			table_name = distributed_storage.getRemoteTableName();
		}
		else
			throw Exception("InJoinSubqueriesPreprocessor: Internal error", ErrorCodes::LOGICAL_ERROR);
	}

	StoragePtr getDistributedSubqueryStorage(const ASTSelectQuery & sub_select_query) const
	{
		if (sub_select_query.table.isNull())
			return {};

		const auto identifier = typeid_cast<const ASTIdentifier *>(sub_select_query.table.get());
		if (identifier == nullptr)
			return {};

		const std::string & table_name = identifier->name;

		std::string database_name;
		if (sub_select_query.database)
			database_name = typeid_cast<const ASTIdentifier &>(*sub_select_query.database).name;
		else
			database_name = "";

		auto subquery_table_storage = context.tryGetTable(database_name, table_name);
		if (!subquery_table_storage)
			return {};

		if (!isEligibleStorageForInJoinPreprocessing<TStorageDistributed>(subquery_table_storage))
			return {};

		return subquery_table_storage;
	}

private:
	ASTSelectQuery * select_query;
	const Context & context;
	const Settings & settings;
	const StoragePtr & storage;
};

}
