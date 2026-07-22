#pragma once
#include <Storages/ColumnsDescription.h>
#include <Common/MultiVersion.h>

namespace DB
{

struct VirtualColumnDescription : public ColumnDescription
{
    VirtualsKind kind = VirtualsKind::None;
    VirtualsMaterializationPlace place = VirtualsMaterializationPlace::Reader;

    VirtualColumnDescription() = default;
    VirtualColumnDescription(String name_, DataTypePtr type_, ASTPtr codec_, String comment_, VirtualsKind kind_, VirtualsMaterializationPlace place_);

    bool isEphemeral() const { return kind == VirtualsKind::Ephemeral; }
    bool isPersistent() const { return kind == VirtualsKind::Persistent; }

    /// This method is needed for boost::multi_index because field
    /// of base class cannot be referenced in boost::multi_index::member.
    const String & getName() const { return name; }
};

class VirtualColumnsDescription
{
public:
    using Container = boost::multi_index_container<
        VirtualColumnDescription,
        boost::multi_index::indexed_by<
            boost::multi_index::sequenced<>,
            boost::multi_index::ordered_unique<boost::multi_index::const_mem_fun<VirtualColumnDescription, const String &, &VirtualColumnDescription::getName>>>>;

    using const_iterator = Container::const_iterator;

    const_iterator begin() const { return container.begin(); }
    const_iterator end() const { return container.end(); }

    VirtualColumnsDescription() = default;

    void add(VirtualColumnDescription desc);
    void addEphemeral(String name, DataTypePtr type, String comment, VirtualsMaterializationPlace place);
    void addPersistent(String name, DataTypePtr type, ASTPtr codec, String comment);
    std::optional<ColumnDefault> getDefault(const String & column_name) const;

    size_t size() const { return container.size(); }
    bool empty() const { return container.empty(); }
    bool has(const String & name) const { return container.get<1>().contains(name); }

    std::optional<NameAndTypePair> tryGet(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const;
    NameAndTypePair get(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const;

    const VirtualColumnDescription * tryGetDescription(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const;
    const VirtualColumnDescription & getDescription(const String & name, VirtualsKind kind, VirtualsMaterializationPlace place) const;

    Block getSampleBlock(VirtualsKind kind, VirtualsMaterializationPlace place) const;

private:
    Container container;
};

using VirtualsDescriptionPtr = std::shared_ptr<const VirtualColumnsDescription>;
using MultiVersionVirtualsDescriptionPtr = MultiVersion<VirtualColumnsDescription>;

}
