#pragma once

#include <Core/Field.h>


namespace DB
{

struct SettingChange
{
    String name;
    Field value;
};


/** Keeps changes of settings, contains a vector SettingChange and supports serialization.
  */
class SettingsChanges
{
    using Container = std::vector<SettingChange>;
public:
    SettingsChanges();
    ~SettingsChanges();

    /// Vector-like access to elements.
    size_t size() const { return changes.size(); }
    SettingChange & operator[](size_t i) { return changes[i]; }
    const SettingChange & operator[](size_t i) const { return changes[i]; }
    Container::iterator begin() { return changes.begin(); }
    Container::iterator end() { return changes.end(); }
    Container::const_iterator begin() const { return changes.begin(); }
    Container::const_iterator end() const { return changes.end(); }
    Container::const_iterator cbegin() const { return changes.cbegin(); }
    Container::const_iterator cend() const { return changes.cend(); }
    void push_back(const SettingChange & change) { changes.emplace_back(change); }
    void push_back(const String & name, const Field & value) { changes.emplace_back(SettingChange{name, value}); }
    SettingChange & back() { return changes.back(); }
    const SettingChange & back() const { return changes.back(); }
    bool empty() const { return changes.empty(); }
    void clear() { changes.clear(); }

    /// Writes changes to buffer. They are serialized as list of contiguous name-value pairs, finished with empty name.
    void serialize(WriteBuffer & buf) const;

    /// Reads changes from buffer.
    void deserialize(ReadBuffer & buf);

private:
    Container changes;
};

}
