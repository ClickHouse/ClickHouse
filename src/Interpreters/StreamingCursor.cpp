#include <Interpreters/StreamingCursor.h>

#include <Common/EventFD.h>
#include <Common/Exception.h>
#include <Core/Streaming/CursorTree.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <ranges>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void StreamingCursor::merge(const CursorTreeNodePtr & cursor)
{
    mergeAt({}, cursor);
}

void StreamingCursor::mergeAt(const Strings & sub_path, const CursorTreeNodePtr & cursor)
{
    if (parent)
    {
        Strings full_path = path;
        full_path.insert(full_path.end(), sub_path.begin(), sub_path.end());
        parent->mergeAt(full_path, cursor);
        return;
    }

    auto wrapped = cursor;
    for (const auto & key : sub_path | std::views::reverse)
    {
        auto node = std::make_shared<CursorTreeNode>();
        node->setSubtree(key, std::move(wrapped));
        wrapped = std::move(node);
    }
    auto canonical = buildCursorTree(cursorTreeToMap(wrapped));

    /// The path in the persisted form, where a key with a dot is split.
    Strings canonical_path;
    for (const auto & key : sub_path)
    {
        Strings parts;
        boost::split(parts, key, boost::is_any_of("."));
        canonical_path.insert(canonical_path.end(), parts.begin(), parts.end());
    }

    std::lock_guard lock(mutex);
    if (!tree)
        tree = std::make_shared<CursorTreeNode>();

    /// The first report under this path replaces the cursor of the previous refresh there.
    if (reported_paths.insert(canonical_path).second)
    {
        if (canonical_path.empty())
        {
            tree = std::make_shared<CursorTreeNode>();
        }
        else
        {
            CursorTreeNode * node = tree.get();
            for (size_t i = 0; i + 1 < canonical_path.size() && node; ++i)
                node = node->hasSubtree(canonical_path[i]) ? node->getSubtree(canonical_path[i]).get() : nullptr;
            if (node && node->hasSubtree(canonical_path.back()))
                node->setSubtree(canonical_path.back(), std::make_shared<CursorTreeNode>());
        }
    }

    mergeCursors(tree, canonical);
}

std::shared_ptr<StreamingCursor> StreamingCursor::createChild(std::shared_ptr<StreamingCursor> parent, Strings path)
{
    auto child = std::make_shared<StreamingCursor>();
    child->parent = std::move(parent);
    child->path = std::move(path);
    return child;
}

StreamingCursor::StreamingCursor() = default;
StreamingCursor::~StreamingCursor() = default;

StreamingCursor & StreamingCursor::root()
{
    StreamingCursor * node = this;
    while (node->parent)
        node = node->parent.get();
    return *node;
}

StreamingCursor::PendingReport::PendingReport(std::shared_ptr<StreamingCursor> holder_)
{
    if (!holder_)
        return;

    /// Keep the root alive: the children of a composite read hold their parents.
    StreamingCursor * root_holder = &holder_->root();
    std::shared_ptr<StreamingCursor> root_ptr(holder_, root_holder);
    {
        std::lock_guard lock(root_holder->mutex);
        ++root_holder->pending_reports;
    }
    holder = std::move(root_ptr);
}

StreamingCursor::PendingReport::PendingReport(PendingReport && other) noexcept
    : holder(std::move(other.holder))
{
}

StreamingCursor::PendingReport & StreamingCursor::PendingReport::operator=(PendingReport && other) noexcept
{
    if (this != &other)
    {
        finish();
        holder = std::move(other.holder);
    }
    return *this;
}

StreamingCursor::PendingReport::~PendingReport()
{
    finish();
}

void StreamingCursor::PendingReport::finish() noexcept
{
    if (!holder)
        return;

    auto finished_holder = std::exchange(holder, nullptr);
    std::lock_guard lock(finished_holder->mutex);
    chassert(finished_holder->pending_reports > 0);
    if (--finished_holder->pending_reports == 0)
    {
#if defined(OS_LINUX) || defined(OS_DARWIN)
        for (const auto & event : finished_holder->no_pending_reports_waiters)
        {
            /// Writing to an eventfd fails only on a counter overflow, which cannot happen for a single write.
            [[maybe_unused]] const bool written = event->write();
            chassert(written);
        }
#endif
        finished_holder->no_pending_reports_waiters.clear();
    }
}

bool StreamingCursor::hasPendingReports()
{
    auto & root_holder = root();
    std::lock_guard lock(root_holder.mutex);
    return root_holder.pending_reports > 0;
}

void StreamingCursor::notifyWhenNoPendingReports(std::shared_ptr<EventFD> event)
{
    auto & root_holder = root();
    std::lock_guard lock(root_holder.mutex);
    if (root_holder.pending_reports > 0)
    {
        root_holder.no_pending_reports_waiters.push_back(std::move(event));
        return;
    }

#if defined(OS_LINUX) || defined(OS_DARWIN)
    if (!event->write())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot notify the end of the pending reports of a streaming cursor");
#endif
}

}
