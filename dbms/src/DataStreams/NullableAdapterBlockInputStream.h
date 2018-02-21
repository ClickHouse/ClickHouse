#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/// Given a column of a block we have just read,
/// how must we process it?
struct Action
{
public:
    enum Type
    {
        /// Do nothing.
        NONE = 0,
        /// Convert nullable column to ordinary column.
        TO_ORDINARY,
        /// Convert non-nullable column to nullable column.
        TO_NULLABLE
    };

    Type type;
    size_t position;
    String new_name;

    static Action create(Type type, size_t position, String new_name);

    void execute(Block & old_block, Block & new_block);
};
/// This stream allows perfoming INSERT requests in which the types of
/// the target and source blocks are compatible up to nullability:
///
/// - if a target column is nullable while the corresponding source
/// column is not, we embed the source column into a nullable column;
/// - if a source column is nullable while the corresponding target
/// column is not, we extract the nested column from the source
/// while checking that is doesn't actually contain NULLs;
/// - otherwise we just perform an identity mapping.
class NullableAdapterBlockInputStream : public IProfilingBlockInputStream
{
public:
    NullableAdapterBlockInputStream(const BlockInputStreamPtr & input, const Block & in_sample_, const Block & out_sample_);

    String getName() const override { return "NullableAdapterBlockInputStream"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override;

private:
    /// Actions to be taken for each column of a block.
    using Actions = std::vector<Action>;

private:
    /// Determine the actions to be taken using the source sample block,
    /// which describes the columns from which we fetch data inside an INSERT
    /// query, and the target sample block which contains the columns
    /// we insert data into.
    void buildActions(const Block & in_sample, const Block & out_sample);

private:
    Block header;
    Actions actions;
    bool must_transform = false;
};

}
