#pragma once

#include <Core/BaseSettings.h>
#include <Storages/SettingDescription.h>

#include <array>
#include <optional>
#include <string_view>

namespace DB
{

/// A settings `Impl` that remembers which source assigned each setting, for the sources a reader cannot
/// recover afterwards: a server config section and `compatibility`, which assign every key they name and so
/// set the changed bit even where the value equals the default, and a named collection, whose values look
/// like any other. Enumeration reports the recorded source instead of `Other`.
///
/// The marks live in the settings object, so they travel with every copy of it - from the server's baseline
/// into each table, from a database into each table it makes. A later `set` clears them, so a setting
/// belongs to whoever assigned it last: the table's own `SETTINGS` clause included. An assignment through
/// `operator[]` bypasses `set` and keeps the mark; engines use that only to adjust a value in place.
///
/// One bit per setting and source; the number of settings is known at compile time, so this allocates nothing.
template <typename TTraits>
struct SettingsWithRecordedOrigin : public BaseSettings<TTraits>
{
    void set(std::string_view name, const Field & value) override
    {
        if (const size_t index = settingIndex(name); index != npos)
            for (auto & bitmap : recorded)
                bitmap[index / 64] &= ~(1ULL << (index % 64));
        BaseSettings<TTraits>::set(name, value);
    }

    /// `set`, then records `origin` for the setting. Only the sources in `recordable_origins`.
    template <SettingOrigin origin>
    void setWithOrigin(std::string_view name, const Field & value)
    {
        constexpr size_t slot = slotOf(origin);
        static_assert(slot < recordable_origins.size(), "this origin is not recorded by `SettingsWithRecordedOrigin`");

        set(name, value);
        if (const size_t index = settingIndex(name); index != npos)
            recorded[slot][index / 64] |= 1ULL << (index % 64);
    }

    std::optional<SettingOrigin> recordedOrigin(std::string_view name) const
    {
        const size_t index = settingIndex(name);
        if (index == npos)
            return {};
        for (size_t i = 0; i < recordable_origins.size(); ++i)
            if (recorded[i][index / 64] & (1ULL << (index % 64)))
                return recordable_origins[i];
        return {};
    }

private:
    static constexpr std::array recordable_origins{SettingOrigin::Config, SettingOrigin::Compatibility, SettingOrigin::NamedCollection};
    static constexpr size_t npos = static_cast<size_t>(-1);
    static constexpr size_t num_words = (static_cast<size_t>(TTraits::SettingID_::NUM_SETTINGS) + 63) / 64;

    std::array<std::array<UInt64, num_words>, recordable_origins.size()> recorded = {};

    static size_t settingIndex(std::string_view name)
    {
        return TTraits::Accessor::instance().find(TTraits::resolveName(name));
    }

    static consteval size_t slotOf(SettingOrigin origin)
    {
        size_t i = 0;
        while (i < recordable_origins.size() && recordable_origins[i] != origin)
            ++i;
        return i;
    }
};

}
