#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

class StorageReplicatedMergeTree;


class ReplicatedMergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	ReplicatedMergeTreeBlockOutputStream(StorageReplicatedMergeTree & storage_, const String & insert_id_, size_t quorum_);

	void writePrefix() override;
	void write(const Block & block) override;

private:
	StorageReplicatedMergeTree & storage;
	String insert_id;
	size_t quorum;
	size_t block_index = 0;

	Logger * log;
};

}
