#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Materializes the columns in the replicated representation (`ColumnReplicated`) of the chunks that pass.
/// With `keep_strings_`, a `ColumnReplicated` over a `ColumnString`, as a `String` column with dictionary
/// granules is read, stays as it is, for a sink that writes such columns (see
/// `IStorage::supportsReplicatedStringColumns`).
class RemovingReplicatedColumnsTransform final : public ISimpleTransform
{
public:
    explicit RemovingReplicatedColumnsTransform(SharedHeader header, bool keep_strings_ = false);

    String getName() const override { return "RemovingReplicatedColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    const bool keep_strings;
};

}
