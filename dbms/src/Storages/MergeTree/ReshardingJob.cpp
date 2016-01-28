#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/ExpressionListParsers.h>

namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

ReshardingJob::ReshardingJob(const std::string & serialized_job)
{
	ReadBufferFromString buf(serialized_job);

	readBinary(database_name, buf);
	readBinary(table_name, buf);
	readBinary(partition, buf);

	std::string expr;
	readBinary(expr, buf);

	IParser::Pos pos = expr.data();
	IParser::Pos max_parsed_pos = pos;
	const char * end = pos + expr.size();

	ParserExpressionWithOptionalAlias parser(false);
	Expected expected = "";
	if (!parser.parse(pos, end, sharding_key_expr, max_parsed_pos, expected))
		throw Exception("ReshardingJob: Internal error", ErrorCodes::LOGICAL_ERROR);

	while (!buf.eof())
	{
		std::string path;
		readBinary(path, buf);

		UInt64 weight;
		readBinary(weight, buf);

		paths.emplace_back(path, weight);
	}
}

ReshardingJob::ReshardingJob(const std::string & database_name_, const std::string & table_name_,
	const std::string & partition_, const WeightedZooKeeperPaths & paths_,
	const ASTPtr & sharding_key_expr_)
	: database_name(database_name_),
	table_name(table_name_),
	partition(partition_),
	paths(paths_),
	sharding_key_expr(sharding_key_expr_)
{
}

std::string ReshardingJob::toString() const
{
	std::string serialized_job;
	WriteBufferFromString buf(serialized_job);

	writeBinary(database_name, buf);
	writeBinary(table_name, buf);
	writeBinary(partition, buf);
	writeBinary(queryToString(sharding_key_expr), buf);

	for (const auto & path : paths)
	{
		writeBinary(path.first, buf);
		writeBinary(path.second, buf);
	}
	buf.next();

	return serialized_job;
}

}
