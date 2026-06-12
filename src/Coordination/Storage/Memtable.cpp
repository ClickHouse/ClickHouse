#include <Coordination/Storage/Memtable.h>

#include <Coordination/Storage/Node.h>

namespace Coordination::Storage
{

std::pair<ChildrenSet2::Set::iterator, bool> ChildrenSet2::insert(
    std::string_view name, NodeAction action, DB::Arena & arena)
{
    auto it = set.find(name);
    if (it == set.end())
    {
        Entry entry;
        entry.ptr = arena.insert(name.data(), name.size());
        entry.len = static_cast<uint32_t>(name.size());
        entry.action = action;
        auto [it2, inserted] = set.insert(entry);
        chassert(inserted);
        return {it2, true};
    }
    else
    {
        return {it, false};
    }
}

void ChildrenSet2::insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict)
{
    auto [it, inserted] = insert(name, action, arena);
    if (!inserted)
    {
        std::optional<NodeAction> combined = combineActions(it->action, action, strict);
        if (!combined)
        {
            /// Create + Remove cancel out, as if the child never existed.
            set.erase(it);
            return;
        }
        if (*combined == it->action)
            return;
        /// Hash set elements are immutable; erase and reinsert with the new action, reusing the
        /// name that's already in the arena.
        Entry entry = *it;
        entry.action = *combined;
        set.erase(it);
        set.insert(entry);
        return;
    }
}

void MemtableChildrenSet::insertCombine(std::string_view name, NodeAction action, DB::Arena & arena, bool strict)
{
    switch (getMode())
    {
        case Mode::Empty:
        {
            ChildrenSet2::Entry entry;
            entry.ptr = arena.insert(name.data(), name.size());
            entry.len = static_cast<uint32_t>(name.size());
            entry.action = action;
            setInlineEntry(entry);
            break;
        }
        case Mode::Inline:
        {
            ChildrenSet2::Entry entry = getInlineEntry();
            if (entry.str() == name)
            {
                std::optional<NodeAction> combined = combineActions(entry.action, action, strict);
                if (!combined)
                {
                    /// Create + Remove: as if the child never existed.
                    mode = Mode::Empty;
                }
                else if (*combined != entry.action)
                {
                    entry.action = *combined;
                    setInlineEntry(entry);
                }
                break;
            }

            auto new_set = std::make_unique<ChildrenSet2>();
            new_set->set.insert(entry); // reuse the name that's already in the arena
            new_set->insertCombine(name, action, arena, strict);
            setSet(new_set.release());
            break;
        }
        case Mode::Set:
            getSet()->insertCombine(name, action, arena, strict);
            break;
    }
}

MemtableChildrenSet::ConstIterator MemtableChildrenSet::iterate() const
{
    switch (getMode())
    {
        case Mode::Empty:
            return {.mode = Mode::Empty, .entry = {}};
        case Mode::Inline:
            return {.mode = Mode::Inline, .entry = getInlineEntry()};
        case Mode::Set:
        {
            const auto * set = getSet();
            return {.mode = Mode::Set, .range = {set->set.begin(), set->set.end()}};
        }
    }
}

bool MemtableChildrenSet::ConstIterator::next(ChildrenSet2::Entry & out)
{
    switch (mode)
    {
        case Mode::Empty:
            return false;
        case Mode::Inline:
            mode = Mode::Empty;
            out = entry;
            return true;
        case Mode::Set:
            if (range.it == range.end)
                return false;
            out = *range.it;
            ++range.it;
            return true;
    }
}

NodeRef Memtable::appendNode(FullNode & node, bool strict)
{
    /// Update the parent's children set. (The root has no parent; Update doesn't change children.)
    if (node.action != NodeAction::Update && node.path.depth != 0)
        children[node.path.parentPath().calculateHash()].insertCombine(
            node.path.baseName(), node.action, arena, strict);

    node_count_delta += nodeCountDelta(node.action);

    BlockPtr new_block;
    NodeRef ref;
    if (BlockData::appendNodeOrStartNewBlock(blocks.empty() ? nullptr : blocks.back(), node, target_block_size, new_block, ref))
    {
        total_bytes += new_block->capacity;
        blocks.push_back(std::move(new_block));
    }
    return ref;
}

bool Memtable::visitChildren(
    const NodePathWithHash & path,
    const std::function<NodeRef(const NodePathWithHash &)> & load_node,
    const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node,
    ChildrenSet2 & seen, DB::Arena & arena_) const
{
    auto lookup = children.find(path.hash);
    if (lookup)
    {
        auto it = lookup->getMapped().iterate();
        std::string child_path_buf;
        FullNode full_node_buf;
        ChildrenSet2::Entry entry;
        while (it.next(entry))
        {
            std::string_view child_name = entry.str();
            const auto [_, inserted] = seen.insert(child_name, entry.action, arena_);
            if (inserted && entry.action != NodeAction::Remove)
            {
                NodeRef ref;
                const FullNode * full_node = nullptr;
                if (load_node)
                {
                    NodePathWithHash child_path = path.path.childPath(child_name, child_path_buf).withCalculatedHash();
                    ref = load_node(child_path);
                    chassert(ref);
                    ref.readWithKnownPath(full_node_buf, child_path);
                    full_node = &full_node_buf;
                }
                if (!check_node(child_name, ref, full_node))
                    return false;
            }
        }
    }
    return true;
}

MemtablePtr Memtable::takeSnapshot() const
{
    MemtablePtr res = std::make_shared<Memtable>();
    res->file_seqno = file_seqno;
    res->total_bytes = total_bytes;
    res->node_count_delta = node_count_delta;
    res->blocks = blocks;

    /// Last block may still be appended to, make an immutable copy.
    if (!res->blocks.empty())
        res->blocks.back() = res->blocks.back()->copyAndShrinkToFit();

    return res;
}

}
