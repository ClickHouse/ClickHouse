#pragma once

#include <Core/Names.h>
#include <Processors/ISimpleTransform.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

class NestedElementsValidationTransform final : public ISimpleTransform
{
public:
    explicit NestedElementsValidationTransform(SharedHeader header);

    String getName() const override { return "NestedElementsValidationTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

/// Members of one `Nested` group whose array sizes must agree row by row: with `reference` if set, else with each other.
struct NestedSizesCheck
{
    Names columns;
    std::optional<String> reference;
};

/// Throws `SIZES_OF_ARRAYS_DONT_MATCH` on the first row that breaks one of `checks`.
class NestedSizesValidationTransform final : public ISimpleTransform
{
public:
    NestedSizesValidationTransform(SharedHeader header, VectorWithMemoryTracking<NestedSizesCheck> checks_);

    String getName() const override { return "NestedSizesValidationTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    VectorWithMemoryTracking<NestedSizesCheck> checks;
};

}

