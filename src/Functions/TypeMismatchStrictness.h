#pragma once

#include <optional>

#include <boost/noncopyable.hpp>


namespace DB
{

/// Whether a function applied to a `Variant`/`Dynamic` column throws on a stored type it cannot handle or
/// resolves the result to NULL is normally taken from `variant_throw_on_type_mismatch` /
/// `dynamic_throw_on_type_mismatch` in the query context of the *current thread*.
///
/// That is the wrong source for callers that only *build* an expression and store the result as table
/// metadata: the verdict then depends on whoever happened to run the DDL, and table loading has no query
/// context at all, so it always sees the strict default and can reject an expression the session that
/// created the table accepted. Such callers pin the strictness with this thread-local override for the
/// duration of the build instead.
bool shouldThrowOnVariantTypeMismatch();
bool shouldThrowOnDynamicTypeMismatch();

class TypeMismatchStrictnessOverride : private boost::noncopyable
{
public:
    TypeMismatchStrictnessOverride(bool variant_throw_on_type_mismatch, bool dynamic_throw_on_type_mismatch);
    ~TypeMismatchStrictnessOverride();

private:
    std::optional<bool> previous_variant;
    std::optional<bool> previous_dynamic;
};

}
