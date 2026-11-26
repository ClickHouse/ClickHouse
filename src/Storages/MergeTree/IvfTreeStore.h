#pragma once

#include <Storages/MergeTree/IvfTree.h>
#include <base/types.h>

#include <functional>
#include <memory>

namespace DB
{

class IDataPartStorage;

/// The trees of centroids of `ivf` indexes: one per table and index, which every part of the table encodes its rows by.
/// They are kept in the table `system.vector_similarity_index_trees`, a row per node, by versions, and cached in memory.
/// The first build of an index of a table trains its tree on its part and stores it; every other part, and every merge,
/// routes and encodes its rows by the stored tree, without training and without keeping its rows aside.
namespace IvfTreeStore
{

struct VersionedTree
{
    UInt64 version = 0;
    std::shared_ptr<const IvfTree> tree;
};

/// The table the part belongs to, the same for all its parts: the directory of the table on its disks.
String tableOf(const IDataPartStorage & part_storage);

/// The latest tree of the index, if there is one.
std::optional<VersionedTree> latest(const String & table, const String & index, size_t dimensions);

/// The latest tree of the index, or, if there is none yet, the one that `train` makes, which becomes version 1. At most
/// one part of a table trains at a time; the others wait for its tree.
VersionedTree latestOrTrain(const String & table, const String & index, size_t dimensions, const std::function<IvfTree()> & train);

/// A version of the tree of the index, which must exist.
std::shared_ptr<const IvfTree> get(const String & table, const String & index, size_t dimensions, UInt64 version);

}

}
