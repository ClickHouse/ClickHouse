#pragma once

#include <string>
#include <DB/Core/Types.h>
#include <DB/Interpreters/SettingsCommon.h>


namespace DB
{

class IAST;
class IStorage;
class ASTSelectQuery;
class Context;


struct Attributes
{
	/// Указатель на начало секции [NOT]IN или JOIN в которой включен этот узел,
	/// если имеется такая секция.
	IAST * enclosing_in_or_join = nullptr;

	/** Глубина одного узла N - это глубина того запроса SELECT, которому принадлежит N.
	 *  Дальше глубина одного запроса SELECT определяется следующим образом:
	 *  - если запрос Q корневой, то select_query_depth(Q) = 0
	 *  - если запрос S является непосредственным подзапросом одного запроса R,
	 *  то select_query_depth(S) = select_query_depth(R) + 1
	 */
	UInt32 select_query_depth = 0;

	bool is_in = false;
	bool is_join = false;

	bool is_global = false;
};

using ASTProperties = std::unordered_map<void *, Attributes>;


class InJoinSubqueriesPreprocessor
{
public:
	InJoinSubqueriesPreprocessor(const Context & context) : context(context) {}
	void process(ASTSelectQuery * query) const;

	/// These methods could be overriden for the need of the unit test.
	virtual bool hasAtLeastTwoShards(const IStorage & table) const;
	virtual std::pair<std::string, std::string> getRemoteDatabaseAndTableName(const IStorage & table) const;
	virtual ~InJoinSubqueriesPreprocessor() {}

private:
	void preprocessSubquery(ASTSelectQuery & sub_select_query, ASTProperties & shadow_ast, SettingDistributedProductMode distributed_product_mode) const;
	IStorage * getDistributedStorage(const ASTSelectQuery & select_query) const;

	const Context & context;
};


}
