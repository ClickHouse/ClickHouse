#pragma once

#include <Storages/SelectQueryInfo.h>

#include <Interpreters/PreparedSets.h>
#include <Core/Field.h>

#include <IO/ReadBufferFromString.h>

namespace DB
{

using FieldVectorPtr = std::shared_ptr<FieldVector>;
using FieldVectorsPtr = std::shared_ptr<std::vector<FieldVector>>;

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Represents a set of keys to iterate. The keys can be multi-column, with each column having a number of value. When iterating via
/// 'advance()', combinations of all the key values can be accessed. For example, built from clause 'WHERE k1 in (1,2,3) AND k2 in (4,5)
/// AND k3 = 7', a key iterator will contain all 6 combinations of key 'k1', 'k2' and 'k3', like [1,4,7], [1,5,7] and so on.
/// The user accesses keys via 'keyValueAt()', then calls 'advance()' to move the iterator to the next key combination.
class KeyIterator
{
public:
    KeyIterator() = delete;

    /// 'begin' specifies the start combination of keys. 'keys_to_process' limits how many combination to iterated before 'atEnd()' returns
    /// true, setting it to 0 means iterating to the last case.
    KeyIterator(FieldVectorsPtr keys_, size_t begin = 0, size_t keys_to_process = 0);

    size_t columns() const { return keys->size(); }

    const Field & keyValueAt(size_t column) const
    {
        assert(column < columns());
        assert(!atEnd());
        return keys->at(column)[key_value_indices[column]];
    }

    void advance();

    bool atEnd() const { return keys_remaining == 0; }

private:
    FieldVectorsPtr keys;
    std::vector<size_t> key_value_indices;
    size_t keys_remaining;
};

/** Retrieve from the query a condition of the form `key = 'key'`, `key in ('xxx_'), from conjunctions in the WHERE clause.
  * TODO support key like search
  */
std::pair<FieldVectorPtr, bool> getFilterKeys(
    const std::string & primary_key, const DataTypePtr & primary_key_type, const SelectQueryInfo & query_info, const ContextPtr & context);

/** Multi-column primary key version.
 * When the primary key is composed of multiple columns, tries to retrieve the key with all the columns having values, returns the pair
 * {founded key values, need to all scan}.
 * For example, the primary key is (k1 Int32, k2 Int32).
 * For 'WHERE k1 IN (1,2,3) AND k2 = 5', returns {[[1,2,3], [5]], false}. The caller can use it to scan all 3 combination of keys.
 * For 'WHERE k1 IN (1,2,3) OR k2 = 5', empty result is returned since no explicit key combination is specified. The caller might want to
 * do full scan.
 * Note that the implementation doesn't support multiple combinations with OR, like 'WHERE (k1 = 1 AND k2 = 2) OR (k1 in (4,5) AND k2 in
 * (2,4))', since deduplication between the combinations will complicates the implementation.
 * Also, empty result might be returned with 'all_scan' set to true, for the clause that explicitly asks for empty key, like
 * 'WHERE k1 IN (1,2) AND k1 = 5'.
 */
std::pair<FieldVectorsPtr, bool> getFilterKeys(
    const Names & primary_key, const DataTypes & primary_key_types, const ActionDAGNodes & filter_nodes, const ContextPtr & context);

template <typename K, typename V>
void fillColumns(const K & key, const V & value, size_t key_pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString key_buffer(key);
    ReadBufferFromString value_buffer(value);
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & serialization = header.getByPosition(i).type->getDefaultSerialization();
        serialization->deserializeBinary(*columns[i], i == key_pos ? key_buffer : value_buffer, {});
    }
}

template <typename S>
void fillColumns(const S & slice, const std::vector<size_t> & pos, const Block & header, MutableColumns & columns)
{
    ReadBufferFromString buffer(slice);
    for (const auto col : pos)
    {
        const auto & serialization = header.getByPosition(col).type->getDefaultSerialization();
        serialization->deserializeBinary(*columns[col], buffer, {});
    }
}

std::vector<std::string> serializeKeysToRawString(
    KeyIterator& key_iterator,
    const DataTypes & key_column_types,
    size_t max_block_size);

std::vector<std::string> serializeKeysToRawString(
    FieldVector::const_iterator & it,
    FieldVector::const_iterator end,
    DataTypePtr key_column_type,
    size_t max_block_size);

std::vector<std::string> serializeKeysToRawString(const ColumnWithTypeAndName & keys);

/// In current implementation key with only column is supported.
size_t getPrimaryKeyPos(const Block & header, const Names & primary_key);

}
