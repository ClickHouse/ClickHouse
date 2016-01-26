#pragma once

#include <DB/Storages/AlterCommands.h>
#include <string>

namespace DB
{

/** Описание задачи перешардирования.
  */
struct ReshardingJob final
{
public:
	/// Создаёт описание на основе его сериализованного представления.
	ReshardingJob(const std::string & serialized_job);

	ReshardingJob(const std::string & database_name_, const std::string & table_name_,
		const std::string & partition_, const WeightedZooKeeperPaths & paths_,
		const std::string & sharding_key_);

	ReshardingJob(const ReshardingJob &) = delete;
	ReshardingJob & operator=(const ReshardingJob &) = delete;

	/// Сериализует описание задачи.
	std::string toString() const;

public:
	std::string database_name;
	std::string table_name;
	std::string partition;
	WeightedZooKeeperPaths paths;
	std::string sharding_key;
};

}
