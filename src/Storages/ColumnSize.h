#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>

namespace DB
{

struct ColumnSize
{
    size_t marks = 0;
    size_t data_compressed = 0;
    size_t data_uncompressed = 0;

    void add(const ColumnSize & other)
    {
        marks += other.marks;
        data_compressed += other.data_compressed;
        data_uncompressed += other.data_uncompressed;
    }
};

using IndexSize = ColumnSize;

struct ColumnPhysicalPresence
{
    size_t parts = 0;
    size_t rows = 0;

    void addPart(size_t part_rows)
    {
        ++parts;
        rows += part_rows;
    }

    void add(const ColumnPhysicalPresence & other)
    {
        parts += other.parts;
        rows += other.rows;
    }
};

struct ColumnPhysicalPresenceByName
{
    size_t total_parts = 0;
    size_t total_rows = 0;
    std::unordered_map<std::string, ColumnPhysicalPresence> columns;

    void clear()
    {
        total_parts = 0;
        total_rows = 0;
        columns.clear();
    }

    void add(const ColumnPhysicalPresenceByName & other)
    {
        total_parts += other.total_parts;
        total_rows += other.total_rows;

        for (const auto & [name, presence] : other.columns)
            columns[name].add(presence);
    }
};
}
