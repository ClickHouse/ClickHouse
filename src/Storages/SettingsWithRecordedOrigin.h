#pragma once

#include <Core/BaseSettings.h>
#include <Storages/SettingDescription.h>
#include <Common/CompactArray.h>

#include <string_view>

namespace DB
{

/// A settings `Impl` that remembers the origin of each setting - who assigned it: a server config section and
/// `compatibility`, which assign every key they name and so set the changed bit even where the value equals the
/// default, a named collection, whose values look like any other, and the table's own `SETTINGS` clause, which
/// each engine's loader records as it applies it. Enumeration reports the recorded origin for a setting that is
/// still changed, so a table's report comes from its settings object alone. The `source` column of
/// `system.table_settings` is that origin.
///
/// The origin lives in the settings object, so it travels with every copy of it - from the server's baseline
/// into each table, from a database into each table it makes. Every assignment records one - `setWithOrigin` the
/// origin it names, `set` `Default`, meaning none - so a setting belongs to whoever assigned it last: the table's
/// own `SETTINGS` clause included, and a reset to the default. An assignment through `operator[]` bypasses both
/// and keeps the origin, which suits an engine that adjusts a value in place, such as by expanding macros; one
/// that replaces a value uses `setAtOffset`, through the typed `set` of its public settings class.
///
/// One 4-bit origin per setting in a `CompactArray`: the number of settings is known at compile time, so this
/// allocates nothing.
template <typename TTraits>
struct SettingsWithRecordedOrigin : public BaseSettings<TTraits>
{
    /// The named collection a loader took values from. A reader may see those values only where it may read
    /// that collection, so the name has to travel with them - `SettingOrigin::NamedCollection` alone says a
    /// collection supplied a value, not which one, and a grant names one. Empty where none supplied any.
    void recordNamedCollection(const String & name) { named_collection = name; }
    const String & recordedNamedCollection() const { return named_collection; }

    /// Assigns `value` and records `origin` as its source; `Default` records none.
    void setWithOrigin(std::string_view name, const Field & value, SettingOrigin origin)
    {
        BaseSettings<TTraits>::set(name, value);
        setOrigin(settingIndex(name), origin);
    }

    /// What `BaseSettings::applyChanges` reaches, recording no source. A loader that applies a source records it
    /// through `setWithOrigin` or `applyChangesWithOrigin` instead.
    void set(std::string_view name, const Field & value) override
    {
        setWithOrigin(name, value, SettingOrigin::Default);
    }

    /// `applyChanges`, recording `origin` for every setting it assigns: for a loader that applies one source
    /// whole, such as the table's own `SETTINGS` clause.
    void applyChangesWithOrigin(const SettingsChanges & changes, SettingOrigin origin)
    {
        for (const auto & change : changes)
        {
            this->checkShorthandChange(change);
            setWithOrigin(change.name, change.value, origin);
        }
    }

    /// The same for the setting whose field is at `offset` in the settings data, as a `SettingIndex` stores it.
    /// For the typed `set` of a public settings class, which holds this behind an incomplete type and so can
    /// pass on only the offset - for an engine that assigns a value itself, over whatever a loader assigned.
    void setAtOffset(size_t offset, const Field & value, SettingOrigin origin = SettingOrigin::Default)
    {
        const auto & accessor = TTraits::Accessor::instance();
        const size_t index = accessor.findByOffset(offset);
        chassert(index != npos);
        accessor.setValue(*this, index, value);
        setOrigin(index, origin);
    }

    /// The declared name of the setting whose field is at `offset`, for code that holds a typed `SettingIndex`
    /// and has to name the setting it points at - matching a row of a described vector, which is keyed by name.
    /// Defined here, where the traits are complete; a settings class surfaces it with
    /// `IMPLEMENT_SETTINGS_NAME_AT_OFFSET`, since its header holds `Impl` behind an incomplete type.
    static std::string_view nameAtOffset(size_t offset)
    {
        const auto & accessor = TTraits::Accessor::instance();
        const size_t index = accessor.findByOffset(offset);
        chassert(index != npos);
        return accessor.getName(index);
    }

    /// Records `origin` for a setting already assigned, without assigning it again: for an engine that applies a
    /// source through another path - a normalised copy of its definition, say - and records it afterwards. An
    /// unknown name is ignored, as `resetToDefault` ignores it.
    void recordOrigin(std::string_view name, SettingOrigin origin) { setOrigin(settingIndex(name), origin); }

    /// A reset forgets the source as well.
    void resetToDefault(std::string_view name) override
    {
        BaseSettings<TTraits>::resetToDefault(name);
        setOrigin(settingIndex(name), SettingOrigin::Default);
    }
    void resetToDefault() override
    {
        BaseSettings<TTraits>::resetToDefault();
        recorded = {};
        named_collection.clear();
    }

    /// The source recorded for the setting, or `Default` when none is, or the name is not a setting.
    SettingOrigin recordedOrigin(std::string_view name) const
    {
        return getOrigin(settingIndex(name));
    }

private:
    /// `Other` is the last of them, which its own comment states, so checking it checks the enum. Spelled out
    /// rather than taken from `magic_enum`, which would pull `EnumReflection.h` into a header that two dozen
    /// settings translation units include, for this one check.
    static_assert(static_cast<size_t>(SettingOrigin::Other) < 16, "a recorded origin is stored in 4 bits");
    static_assert(static_cast<UInt8>(SettingOrigin::Default) == 0,
                  "a zeroed `CompactArray` must mean nothing recorded");

    static constexpr size_t npos = static_cast<size_t>(-1);

    static constexpr size_t num_settings = static_cast<size_t>(TTraits::SettingID_::NUM_SETTINGS);

    CompactArray<size_t, 4, num_settings> recorded;
    String named_collection;

    /// An index out of range is a name `BaseSettings` does not know: `resetToDefault` ignores it, and `set`
    /// throws `UNKNOWN_SETTING` before recording, or stores a custom setting, which has no origin to record,
    /// where the traits allow those. So it is skipped here as the base skips it.
    void setOrigin(size_t index, SettingOrigin origin)
    {
        if (index < num_settings)
            recorded.set(index, static_cast<UInt8>(origin));
    }

    /// The source recorded for the setting, or `Default` when none is. For an unknown name, too, as
    /// `BaseSettings::isChanged` answers `false` for one rather than throwing.
    SettingOrigin getOrigin(size_t index) const
    {
        return index >= num_settings ? SettingOrigin::Default : static_cast<SettingOrigin>(recorded.get(index));
    }

    static size_t settingIndex(std::string_view name)
    {
        return TTraits::Accessor::instance().find(TTraits::resolveName(name));
    }
};

/// `IMPLEMENT_SETTINGS_TRAITS` with an `Impl` that records where each value came from. An `Impl` that needs
/// methods of its own - `MergeTreeSettingsImpl` - is written out and takes `IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL`.
/// NOLINTNEXTLINE
#define IMPLEMENT_SETTINGS_TRAITS_WITH_RECORDED_ORIGIN(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE) \
    struct CLASS_NAME##Impl : public SettingsWithRecordedOrigin<SETTINGS_TRAITS_NAME> {}; \
    IMPLEMENT_SETTINGS_TRAITS_CUSTOM_IMPL(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO, CLASS_NAME, SETTING_NAMESPACE)

}
