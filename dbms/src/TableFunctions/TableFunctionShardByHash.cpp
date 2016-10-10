#include <DB/TableFunctions/getStructureOfRemoteTable.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Interpreters/evaluateConstantExpression.h>
#include <DB/Interpreters/Cluster.h>
#include <DB/Interpreters/getClusterName.h>
#include <DB/Common/SipHash.h>
#include <DB/TableFunctions/TableFunctionShardByHash.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
	extern const int BAD_ARGUMENTS;
}

StoragePtr TableFunctionShardByHash::execute(ASTPtr ast_function, Context & context) const
{
	ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

	const char * err = "Table function 'shardByHash' requires 4 parameters: "
		"cluster name, key string to hash, name of remote database, name of remote table.";

	if (args_func.size() != 1)
		throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

	if (args.size() != 4)
		throw Exception(err, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	String cluster_name;
	String key;
	String remote_database;
	String remote_table;

	auto getStringLiteral = [](const IAST & node, const char * description)
	{
		const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(&node);
		if (!lit)
			throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

		if (lit->value.getType() != Field::Types::String)
			throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

		return safeGet<const String &>(lit->value);
	};

	cluster_name = getClusterName(*args[0]);
	key = getStringLiteral(*args[1], "Key to hash");

	args[2] = evaluateConstantExpressionOrIdentidierAsLiteral(args[2], context);
	args[3] = evaluateConstantExpressionOrIdentidierAsLiteral(args[3], context);

	remote_database = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();
	remote_table = static_cast<const ASTLiteral &>(*args[3]).value.safeGet<String>();

	/// Аналогично другим TableFunctions.
	for (auto & arg : args)
		if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(arg.get()))
			id->kind = ASTIdentifier::Table;

	auto cluster = context.getCluster(cluster_name);
	size_t shard_index = sipHash64(key) % cluster->getShardCount();

	std::shared_ptr<Cluster> shard(cluster->getClusterWithSingleShard(shard_index).release());

	return StorageDistributed::create(
		getName(),
		std::make_shared<NamesAndTypesList>(getStructureOfRemoteTable(*shard, remote_database, remote_table, context)),
		remote_database,
		remote_table,
		shard,
		context);
}

}
