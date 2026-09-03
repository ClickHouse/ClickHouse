#pragma once

#include <Interpreters/StorageID.h>

#include <fmt/format.h>

namespace DB
{

// This class just releases some restrictions from StorageID
// StorageIDMaybeEmpty has default c-tor implemented as StorageID::createEmpty()
// It is safe to compare empty instances of this class with each other and with other StorageID instances.
struct StorageIDMaybeEmpty : public StorageID
{

    using StorageID::StorageID;

    StorageIDMaybeEmpty();
    StorageIDMaybeEmpty(const StorageID & other); // NOLINT this is an implicit c-tor

    bool operator==(const StorageIDMaybeEmpty & other) const;
};

}

namespace fmt
{

template <>
struct formatter<DB::StorageIDMaybeEmpty>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const DB::StorageIDMaybeEmpty & storage_id, FormatContext & ctx) const
    {
        if (storage_id)
            return fmt::format_to(ctx.out(), "{}", storage_id.getFullTableName());
        return fmt::format_to(ctx.out(), "{}", "<empty>");
    }
};

}
