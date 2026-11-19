#pragma once

#include <Common/ListWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Processors/IProcessor.h>

#include <optional>

namespace DB
{

class Block;
struct FormatSettings;

/// The number of leaf columns a columnar file stores for `header` (each Tuple element, Map keys and values, the nested column
/// of Array, Nullable and LowCardinality), or nullopt for a `JSON` or `Dynamic` column, whose number depends on the file.
std::optional<size_t> countLeafColumns(const Block & header);

/// The leaf columns a reader of `format_header` reads, an `AggregateFunction` parsed from values (`aggregate_function_input_format`)
/// counting as its values, or nullopt if unknown: always for a Vortex file, which reads whole top-level fields and converts them.
std::optional<size_t> countLeafColumnsToRead(const Block & format_header, const FormatSettings & format_settings, bool is_vortex);

/// The lazy branch of lazy materialization for files (object storage and `File`, see optimizeLazyMaterialization2).
/// The derived class creates the readers of the deferred columns in updatePipeline(), when the surviving rows are known
/// (after the main branch with the `LIMIT` is fully executed); until then, prepare() reports UpdatePipeline.
/// The files are read by up to `max_threads` readers, each over a contiguous group of files, and the chunks are returned
/// in the order of the groups, so the rows stay sorted by the global row index.
class LazyReadFromFileGroupsSource : public IProcessor
{
public:
    Status prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & updated_output_ports) final;

protected:
    LazyReadFromFileGroupsSource(SharedHeader header, size_t max_threads_, size_t main_read_leaf_columns_);

    /// Readers for `num_files` files: at most `max_threads` and one per file, and all of them together read no more leaf
    /// columns at once than the `main_read_streams` sources of the main branch. A read of unknown width keeps one reader.
    size_t getNumReaders(size_t main_read_streams, std::optional<size_t> lazy_read_leaf_columns, size_t num_files) const;

    /// Contiguous groups of files in file order, with about the same number of rows and at least one file each.
    template <typename FileRows>
    static VectorWithMemoryTracking<VectorWithMemoryTracking<FileRows>> splitIntoGroups(VectorWithMemoryTracking<FileRows> files, size_t num_groups);

    /// Connects the readers, one per group in the order of the groups (none if no rows survived the `LIMIT`).
    PipelineUpdate addReaders(Processors readers);

private:
    const size_t max_threads;
    const size_t main_read_leaf_columns;
    bool readers_added = false;

    /// The chunks of a reader that are pulled before the readers of the previous groups have finished.
    VectorWithMemoryTracking<ListWithMemoryTracking<Chunk>> chunks;
    UnorderedMapWithMemoryTracking<const InputPort *, size_t> input_port_to_index;
    size_t next_chunk_to_process = 0;
    InputPorts::iterator next_input_to_process;
};

template <typename FileRows>
VectorWithMemoryTracking<VectorWithMemoryTracking<FileRows>> LazyReadFromFileGroupsSource::splitIntoGroups(
    VectorWithMemoryTracking<FileRows> files, size_t num_groups)
{
    size_t total_rows = 0;
    for (const auto & file : files)
        total_rows += file.rows->size();

    VectorWithMemoryTracking<VectorWithMemoryTracking<FileRows>> groups(num_groups);
    size_t group = 0;
    size_t rows_up_to_current_file = 0;
    for (size_t i = 0; i < files.size(); ++i)
    {
        rows_up_to_current_file += files[i].rows->size();
        groups[group].push_back(std::move(files[i]));
        const size_t files_left = files.size() - i - 1;
        const size_t groups_left = num_groups - group - 1;
        if (groups_left > 0 && (files_left == groups_left || rows_up_to_current_file * num_groups >= total_rows * (group + 1)))
            ++group;
    }
    return groups;
}

}
