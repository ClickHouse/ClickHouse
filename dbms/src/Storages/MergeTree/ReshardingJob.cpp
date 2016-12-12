#include <DB/Storages/MergeTree/ReshardingJob.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/Parsers/ParserQuery.h>
#include <DB/Parsers/ExpressionListParsers.h>
#include <DB/Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

ReshardingJob::ReshardingJob(const std::string & serialized_job)
{
	ReadBufferFromString buf{serialized_job};

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
		throw Exception{"ReshardingJob: Internal error", ErrorCodes::LOGICAL_ERROR};

	readBinary(coordinator_id, buf);
	readVarUInt(block_number, buf);
	readBinary(do_copy, buf);

	size_t s;
	readVarUInt(s, buf);

	for (size_t i = 0; i < s; ++i)
	{
		std::string path;
		readBinary(path, buf);

		UInt64 weight;
		readVarUInt(weight, buf);

		paths.emplace_back(path, weight);
	}
}

ReshardingJob::ReshardingJob(const std::string & database_name_, const std::string & table_name_,
	const std::string & partition_, const WeightedZooKeeperPaths & paths_,
	const ASTPtr & sharding_key_expr_, const std::string & coordinator_id_)
	: database_name{database_name_},
	table_name{table_name_},
	partition{partition_},
	paths{paths_},
	sharding_key_expr{sharding_key_expr_},
	coordinator_id{coordinator_id_}
{
}

ReshardingJob::operator bool() const
{
	return !database_name.empty()
		&& !table_name.empty()
		&& !partition.empty()
		&& !paths.empty()
		&& (storage != nullptr);
}

std::string ReshardingJob::toString() const
{
	std::string serialized_job;
	WriteBufferFromString buf{serialized_job};

	writeBinary(database_name, buf);
	writeBinary(table_name, buf);
	writeBinary(partition, buf);
	writeBinary(queryToString(sharding_key_expr), buf);
	writeBinary(coordinator_id, buf);
	writeVarUInt(block_number, buf);
	writeBinary(do_copy, buf);

	writeVarUInt(paths.size(), buf);
	for (const auto & path : paths)
	{
		writeBinary(path.first, buf);
		writeVarUInt(path.second, buf);
	}
	buf.next();

	return serialized_job;
}

bool ReshardingJob::isCoordinated() const
{
	return !coordinator_id.empty();
}

void ReshardingJob::clear()
{
	database_name.clear();
	table_name.clear();
	partition.clear();
	paths.clear();
	coordinator_id.clear();
	storage = nullptr;
	block_number = 0;
	is_aborted = false;
}

}
