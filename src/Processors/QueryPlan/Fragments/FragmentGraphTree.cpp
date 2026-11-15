#include <Processors/QueryPlan/Fragments/FragmentGraphExplain.h>

#include <Processors/QueryPlan/Fragments/FragmentGraph.h>
#include <Processors/QueryPlan/Fragments/PreparedFragmentGraph.h>
#include <Processors/QueryPlan/Fragments/TextCanvas.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanFormat.h>
#include <IO/Operators.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <limits>
#include <map>
#include <unordered_map>
#include <Common/MapWithMemoryTracking.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// The text of a step box (its name and description) is wrapped to lines of this many columns.
constexpr size_t max_step_text_width = 40;
/// The detail lines of a step box are cut to this many columns.
constexpr size_t max_detail_width = 60;
/// The first line of an exchange label (name and kind) is cut to this many columns.
constexpr size_t max_label_width = 48;
/// Columns between a step box and the frame of its fragment.
constexpr Int64 frame_padding = 2;
struct StepBox
{
    /// The whole text of the step: its name and description.
    String text;
    /// `text` wrapped to `max_step_text_width` columns, one line per inner line of the box.
    VectorWithMemoryTracking<String> lines;
    /// The lines `EXPLAIN` prints under the step, drawn under the text.
    VectorWithMemoryTracking<String> details;
    /// Index in `FragmentGraph::fragments`.
    size_t fragment = 0;
    /// Inputs in the plan, then, for a receive step, the send step of the producer fragment.
    VectorWithMemoryTracking<size_t> children;
    /// Set for the root box of a fragment that is drawn under a receive step: index in `FragmentGraph::exchanges`.
    std::optional<size_t> incoming_exchange;
    size_t width = 0;
    Int64 center = 0;
    size_t y = 0;

    Int64 left() const { return center - static_cast<Int64>(width / 2); }
    Int64 right() const { return left() + static_cast<Int64>(width) - 1; }
    size_t height() const { return TextCanvas::boxHeight(lines.size(), details.size()); }
    /// The line of the bottom border.
    size_t bottom() const { return y + height() - 1; }

    /// Sets `text`, wraps it into `lines` and updates `width`.
    void setText(String new_text)
    {
        text = std::move(new_text);
        lines = TextCanvas::wrapToWidth(text, max_step_text_width);
        updateWidth();
    }

    void updateWidth()
    {
        width = 0;
        for (const auto & line : lines)
            width = std::max(width, TextCanvas::displayWidth(line));
        for (const auto & detail : details)
            width = std::max(width, TextCanvas::displayWidth(detail));
        width += 4;
    }
};

/// A nested plan of a step (`IQueryPlanStep::getChildPlans`), drawn after the main drawing as a plan of its own. Its id
/// is `N<index + 1>`.
struct NestedPlan
{
    const QueryPlan * plan = nullptr;
    /// The name and description of the step that owns the plan.
    String owner;
};

using NestedPlans = VectorWithMemoryTracking<NestedPlan>;

struct FragmentFrame
{
    String title;
    size_t root_box = 0;
    size_t bottom = 0;
};

class TreeBuilder
{
public:
    /// The nested plans of the drawn steps are appended to `nested_plans_`, which is shared by the builders of the main
    /// drawing and of the nested plans, so that the ids are unique in the whole output.
    TreeBuilder(
        const PreparedFragmentGraph & prepared_, size_t max_step_description_length_, NestedPlans & nested_plans_, bool frameless_)
        : prepared(prepared_)
        , graph(prepared.graph)
        , options(prepared.options)
        , nested_plans(nested_plans_)
        , frameless(frameless_)
        , padding(frameless ? 0 : frame_padding)
        , sibling_gap(3 + 2 * padding)
        , max_step_description_length(max_step_description_length_)
    {
        for (size_t i = 0; i < graph.fragments.size(); ++i)
            fragment_index_by_id[graph.fragments[i].id] = i;
    }

    /// Draws the fragments of the graph and the exchanges between them.
    void drawGraph(WriteBuffer & buf)
    {
        buildBoxes();
        attachProducers();
        drawFrom(rootFragment(), buf);
    }

    /// Draws a nested plan as bare boxes. Must be built with `frameless`.
    void drawNestedPlan(const QueryPlan & plan, WriteBuffer & buf)
    {
        chassert(frameless);
        if (!plan.getRootNode())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A nested plan to draw has no root node");
        frames.resize(1);
        frames[0].root_box = addPlanBoxes(*plan.getRootNode(), 0, prepared.getPrettyNames(plan));
        drawFrom(0, buf);
    }

private:
    void drawFrom(size_t root_fragment, WriteBuffer & buf)
    {
        const size_t root = frames[root_fragment].root_box;
        layoutColumns(root, 0);
        Int64 min_left = 0;
        for (size_t i = 0; i < frames.size(); ++i)
            min_left = std::min(min_left, frameLeft(i));
        if (min_left < 0)
            shiftSubtree(root, -min_left);

        /// The title of the root frame is on line 0, its first box on line 2. Without a frame the first box is on line 0.
        layoutRows(root_fragment, frameless ? 0 : 2);

        TextCanvas canvas;
        if (!frameless)
            for (size_t i = 0; i < frames.size(); ++i)
                drawFrame(canvas, i);
        for (const auto & step_box : boxes)
            canvas.box(static_cast<size_t>(step_box.left()), step_box.y, step_box.width, step_box.lines, step_box.details);
        for (size_t i = 0; i < boxes.size(); ++i)
            drawEdges(canvas, i);
        canvas.writeTo(buf);
    }

    const PreparedFragmentGraph & prepared;
    const FragmentGraph & graph;
    const ExplainPlanOptions & options;
    NestedPlans & nested_plans;
    /// A plan that was not cut is one fragment, drawn as bare boxes: no frame, no title, no padding for the frame.
    const bool frameless;
    /// Columns between a step box and the frame of its fragment: `frame_padding`, or 0 without a frame.
    const Int64 padding;
    /// Columns between the subtrees of two inputs of a step; the subtrees already include the padding.
    const Int64 sibling_gap;
    /// A description is cut to this many bytes, as in `QueryPlan::explainPlan`; 0 means no limit.
    const size_t max_step_description_length;
    UnorderedMapWithMemoryTracking<UInt32, size_t> fragment_index_by_id;

    VectorWithMemoryTracking<StepBox> boxes;
    VectorWithMemoryTracking<FragmentFrame> frames;
    /// (fragment index, exchange name) -> the receive step box of that exchange.
    MapWithMemoryTracking<std::pair<size_t, String>, size_t> receive_boxes;

    size_t fragmentIndex(UInt32 id) const
    {
        auto it = fragment_index_by_id.find(id);
        if (it == fragment_index_by_id.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} is not found in the fragment graph", id);
        return it->second;
    }

    /// The fragment that no exchange reads. The drawing hangs every other fragment under it, so it must be the only one:
    /// a second root would never be laid out.
    size_t rootFragment() const
    {
        VectorWithMemoryTracking<bool> is_producer(graph.fragments.size(), false);
        for (const auto & exchange : graph.exchanges)
            is_producer[fragmentIndex(exchange.from)] = true;
        VectorWithMemoryTracking<UInt32> roots;
        for (size_t i = 0; i < graph.fragments.size(); ++i)
            if (!is_producer[i])
                roots.push_back(graph.fragments[i].id);
        if (roots.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot draw the fragment graph: it must have exactly one fragment that no exchange reads, found [{}]",
                fmt::join(roots, ", "));
        return fragmentIndex(roots.front());
    }

    /// The name of the step and its description. The nested plans of the step are registered in `nested_plans`, to be
    /// drawn after the main drawing, and the text ends with their ids: `← N1, N2`. Must be called once per step.
    String stepText(IQueryPlanStep & step)
    {
        String text = step.getName();
        String description(step.getStepDescription());
        if (options.pretty)
            description = QueryPlanFormat::trimColumnIdentifier(description);
        if (max_step_description_length)
            description = description.substr(0, max_step_description_length);
        if (options.description && !description.empty())
            text += fmt::format(" ({})", description);

        VectorWithMemoryTracking<String> ids;
        for (const auto * child_plan : step.getChildPlans(/*for_explain=*/ true))
        {
            /// `QueryPlanFormat::buildPrettyNamesPerPlan` skips such plans too.
            if (!child_plan || !child_plan->getRootNode())
                continue;
            nested_plans.push_back({child_plan, text});
            ids.push_back(fmt::format("N{}", nested_plans.size()));
        }
        if (!ids.empty())
            text += fmt::format(" ← {}", fmt::join(ids, ", "));
        return text;
    }

    /// The lines plain `EXPLAIN` prints under the step with the same options, each cut to `max_detail_width`.
    VectorWithMemoryTracking<String> stepDetails(IQueryPlanStep & step, const PrettyNames & names) const
    {
        WriteBufferFromOwnString out;
        IQueryPlanStep::FormatSettings settings{
            .out = out,
            .header_prefix = "",
            .detail_prefix = "",
            .write_header = options.header,
            .compact = options.compact,
            .pretty = options.pretty,
            .pretty_names = names.pretty_names,
            .runtime_filter_names = names.runtime_filter_names};
        explainStepDetails(step, settings, options);

        VectorWithMemoryTracking<String> details;
        std::string_view text = out.str();
        while (!text.empty())
        {
            size_t end = text.find('\n');
            std::string_view line = text.substr(0, end);
            if (!line.empty())
                details.push_back(TextCanvas::cutToWidth(line, max_detail_width));
            if (end == std::string_view::npos)
                break;
            text.remove_prefix(end + 1);
        }
        return details;
    }

    size_t addBox(String text, VectorWithMemoryTracking<String> details, size_t fragment)
    {
        StepBox step_box;
        step_box.details = std::move(details);
        step_box.setText(std::move(text));
        step_box.fragment = fragment;
        boxes.push_back(std::move(step_box));
        return boxes.size() - 1;
    }

    /// An `Expression` step is never drawn: the box of its input takes its place, as in plain `EXPLAIN` with `compact`.
    static const QueryPlan::Node & skipExpressions(const QueryPlan::Node & node)
    {
        const QueryPlan::Node * current = &node;
        while (current->step->getName() == "Expression" && current->children.size() == 1)
            current = current->children.front();
        return *current;
    }

    size_t addPlanBoxes(const QueryPlan::Node & plan_node, size_t fragment, const PrettyNames & names)
    {
        const auto & node = skipExpressions(plan_node);
        size_t index = addBox(stepText(*node.step), stepDetails(*node.step, names), fragment);
        if (const auto * exchange_id = PreparedFragmentGraph::tryGetReceiveExchangeId(*node.step))
            receive_boxes[{fragment, *exchange_id}] = index;
        for (const auto * child : node.children)
        {
            size_t child_index = addPlanBoxes(*child, fragment, names);
            boxes[index].children.push_back(child_index);
        }
        return index;
    }

    void buildBoxes()
    {
        frames.resize(graph.fragments.size());
        for (size_t i = 0; i < graph.fragments.size(); ++i)
        {
            const auto & fragment = graph.fragments[i];
            auto & frame = frames[i];
            frame.title = fmt::format("F{} · ", fragment.id);
            frame.title += fragment.description ? "set: " + *fragment.description : fragment.stage_name;
            if (fragment.execution_mode == QueryFragment::ExecutionMode::SingleNode)
                frame.title += " · single node";
            if (fragment.fallback_reason)
                frame.title += " · not distributed: " + *fragment.fallback_reason;
            frame.title = TextCanvas::replaceControlCharacters(frame.title);

            if (!fragment.plan || !fragment.plan->getRootNode())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to draw", fragment.id);
            frames[i].root_box = addPlanBoxes(*fragment.plan->getRootNode(), i, prepared.getPrettyNames(*fragment.plan));
        }
    }

    /// The distance of a fragment from the root fragment along its longest chain of consumers.
    size_t fragmentDepth(size_t fragment, VectorWithMemoryTracking<std::optional<size_t>> & depths) const
    {
        if (depths[fragment])
            return *depths[fragment];
        size_t depth = 0;
        for (const auto & exchange : graph.exchanges)
            if (exchange.from == graph.fragments[fragment].id)
                depth = std::max(depth, fragmentDepth(fragmentIndex(exchange.to), depths) + 1);
        depths[fragment] = depth;
        return depth;
    }

    /// The box of `consumer` that an exchange ends at: the receive step of the exchange.
    size_t findReceiveBox(size_t consumer, const FragmentExchange & exchange) const
    {
        auto it = receive_boxes.find({consumer, exchange.name});
        if (it == receive_boxes.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Cannot draw exchange {}: fragment {} has no receive step for it", exchange.name, exchange.to);
        return it->second;
    }

    /// Hangs every fragment under one consumer: the deepest one, so that the fragment is drawn below all its readers.
    /// The other consumers get a note in their receive step and in the producer's title instead of a second edge.
    void attachProducers()
    {
        VectorWithMemoryTracking<std::optional<size_t>> depths(graph.fragments.size());
        VectorWithMemoryTracking<std::optional<size_t>> primary_exchange(graph.fragments.size());
        for (size_t i = 0; i < graph.exchanges.size(); ++i)
        {
            const auto & exchange = graph.exchanges[i];
            size_t producer = fragmentIndex(exchange.from);
            size_t consumer = fragmentIndex(exchange.to);
            auto & primary = primary_exchange[producer];
            if (!primary)
            {
                primary = i;
                continue;
            }
            size_t primary_consumer = fragmentIndex(graph.exchanges[*primary].to);
            size_t depth = fragmentDepth(consumer, depths);
            size_t primary_depth = fragmentDepth(primary_consumer, depths);
            if (depth > primary_depth || (depth == primary_depth && exchange.to < graph.exchanges[*primary].to))
                primary = i;
        }

        for (size_t i = 0; i < graph.exchanges.size(); ++i)
        {
            const auto & exchange = graph.exchanges[i];
            size_t producer = fragmentIndex(exchange.from);
            size_t consumer = fragmentIndex(exchange.to);
            const size_t receive_box = findReceiveBox(consumer, exchange);

            if (primary_exchange[producer] == i)
            {
                boxes[receive_box].children.push_back(frames[producer].root_box);
                boxes[frames[producer].root_box].incoming_exchange = i;
            }
            else
            {
                auto & step_box = boxes[receive_box];
                step_box.setText(fmt::format("{} ← F{}", step_box.text, exchange.from));
                frames[producer].title += fmt::format(" · also read by F{}", exchange.to);
            }
        }
    }

    bool crossesFragments(size_t parent, size_t child) const { return boxes[parent].fragment != boxes[child].fragment; }

    VectorWithMemoryTracking<String> labelLines(size_t exchange_index) const
    {
        const auto & exchange = graph.exchanges[exchange_index];
        VectorWithMemoryTracking<String> lines;
        lines.push_back(TextCanvas::cutToWidth(
            fmt::format("{} · {}", exchange.name, prepared.formatExchangeKind(exchange)), max_label_width));
        String mode(PreparedFragmentGraph::formatExchangeMode(exchange.mode));
        /// A persisted exchange is written to storage before its consumer starts: `⛁` marks it next to the mode, which
        /// still says it in words, so a terminal without the glyph loses nothing but the mark.
        if (exchange.mode == ExchangeDescription::Kind::Persisted)
            mode = "⛁ " + mode;
        if (exchange.source_buckets || exchange.destination_buckets)
            mode += fmt::format(" · {} → {}", exchange.source_buckets, exchange.destination_buckets);
        lines.push_back(std::move(mode));
        if (options.estimates && exchange.statistics.estimated_rows)
            lines.push_back(fmt::format("~{} rows", *exchange.statistics.estimated_rows));
        return lines;
    }

    size_t labelWidth(size_t exchange_index) const
    {
        size_t width = 0;
        for (const auto & line : labelLines(exchange_index))
            width = std::max(width, TextCanvas::displayWidth(line));
        return width;
    }

    void shiftSubtree(size_t index, Int64 offset)
    {
        boxes[index].center += offset;
        for (size_t child : boxes[index].children)
            shiftSubtree(child, offset);
    }

    Int64 frameLeft(size_t fragment) const
    {
        Int64 left = std::numeric_limits<Int64>::max();
        for (const auto & step_box : boxes)
            if (step_box.fragment == fragment)
                left = std::min(left, step_box.left());
        left -= padding;

        /// The edge into the fragment crosses the top border at the centre of the root box: `┌─ title ` must end before.
        const auto & root_box = boxes[frames[fragment].root_box];
        if (root_box.incoming_exchange)
            left = std::min(left, root_box.center - static_cast<Int64>(TextCanvas::displayWidth(frames[fragment].title)) - 4);
        return left;
    }

    Int64 frameRight(size_t fragment) const
    {
        Int64 right = std::numeric_limits<Int64>::min();
        for (const auto & step_box : boxes)
            if (step_box.fragment == fragment)
                right = std::max(right, step_box.right());
        if (frameless)
            return right;
        /// `┌─ title ┐` needs the title and 5 more columns.
        return std::max(right + frame_padding, frameLeft(fragment) + static_cast<Int64>(TextCanvas::displayWidth(frames[fragment].title)) + 4);
    }

    /// Places the subtree of `index` in the columns from `left` on, as a tidy tree: leaves left to right, a step centred
    /// between its first and last input. Returns the first free column to the right of the subtree.
    Int64 layoutColumns(size_t index, Int64 left)
    {
        auto & step_box = boxes[index];
        const auto width = static_cast<Int64>(step_box.width);
        /// The right bound of a leaf; a step with inputs computes it from the subtrees of its inputs below.
        Int64 right = left + width + 2 * padding;
        if (step_box.children.empty())
        {
            step_box.center = left + padding + width / 2;
        }
        else
        {
            Int64 x = left;
            for (size_t child : step_box.children)
                x = layoutColumns(child, x) + sibling_gap;
            right = x - sibling_gap;

            auto & placed = boxes[index];
            placed.center = (boxes[placed.children.front()].center + boxes[placed.children.back()].center) / 2;
            /// A box wider than its inputs must not cross the left bound of its subtree.
            Int64 overflow = left + padding - placed.left();
            if (overflow > 0)
            {
                shiftSubtree(index, overflow);
                right += overflow;
            }
            right = std::max(right, placed.right() + 1 + padding);
        }

        /// The frame of a fragment that an edge enters reaches left of its boxes when its title is long (see `frameLeft`), and
        /// must not cross the left bound of the subtree either.
        if (frames[boxes[index].fragment].root_box == index)
        {
            Int64 overflow = left - frameLeft(boxes[index].fragment);
            if (overflow > 0)
            {
                shiftSubtree(index, overflow);
                right += overflow;
            }
        }

        const auto & placed = boxes[index];
        /// The label of the edge into this fragment is written to the right of the edge, inside this subtree.
        if (placed.incoming_exchange)
            right = std::max(right, placed.center + 2 + static_cast<Int64>(labelWidth(*placed.incoming_exchange)) + 1);
        /// The title of the frame must fit as well.
        if (frames[placed.fragment].root_box == index)
            right = std::max(right, frameRight(placed.fragment) + 1);
        return right;
    }

    /// Assigns lines to the boxes of `fragment`, whose root box is on line `top_box_y`, and then to the fragments
    /// drawn under it.
    void layoutRows(size_t fragment, size_t top_box_y)
    {
        VectorWithMemoryTracking<size_t> producers;
        size_t bottom = 0;
        bool branches_to_producers = false;

        VectorWithMemoryTracking<std::pair<size_t, size_t>> stack{{frames[fragment].root_box, top_box_y}};
        while (!stack.empty())
        {
            auto [index, y] = stack.back();
            stack.pop_back();
            boxes[index].y = y;
            bottom = std::max(bottom, boxes[index].bottom());

            const auto & children = boxes[index].children;
            size_t in_fragment = 0;
            size_t to_producers = 0;
            for (size_t child : children)
            {
                if (crossesFragments(index, child))
                {
                    producers.push_back(child);
                    ++to_producers;
                }
                else
                    ++in_fragment;
            }
            if (to_producers > 1)
                branches_to_producers = true;
            /// One input: `▲`, `│`, then the input box. Several: one more line for the branch.
            size_t child_y = boxes[index].bottom() + 1 + (in_fragment > 1 ? 2 : 1);
            for (size_t child : children)
                if (!crossesFragments(index, child))
                    stack.emplace_back(child, child_y);
        }

        auto & frame = frames[fragment];
        frame.bottom = bottom + 1;

        size_t label_height = 0;
        for (size_t producer_root : producers)
            label_height = std::max(label_height, labelLines(*boxes[producer_root].incoming_exchange).size());

        /// Under the frame: the branch line if one box has several producers, the labels, the producer frame top
        /// with its title, an empty line, then the first box of the producer.
        size_t producer_top = frame.bottom + 1 + (branches_to_producers ? 1 : 0) + label_height;
        for (size_t producer_root : producers)
            layoutRows(boxes[producer_root].fragment, producer_top + 2);
    }

    void drawFrame(TextCanvas & canvas, size_t fragment)
    {
        const auto & frame = frames[fragment];
        Int64 left = frameLeft(fragment);
        Int64 right = frameRight(fragment);
        size_t top = boxes[frame.root_box].y - 2;
        canvas.frame(static_cast<size_t>(left), top, static_cast<size_t>(right - left + 1), frame.bottom - top + 1, frame.title);
    }

    /// The branch line under a step with several inputs: `┌───┴───┐`, with `┬` above the middle inputs.
    static void drawBranch(TextCanvas & canvas, size_t y, Int64 parent_center, const VectorWithMemoryTracking<Int64> & child_centers)
    {
        for (Int64 x = child_centers.front(); x <= child_centers.back(); ++x)
            canvas.put(static_cast<size_t>(x), y, "─");
        for (size_t i = 0; i < child_centers.size(); ++i)
        {
            const char * glyph = i == 0 ? "┌" : (i + 1 == child_centers.size() ? "┐" : "┬");
            canvas.put(static_cast<size_t>(child_centers[i]), y, glyph);
        }
        bool parent_over_child = std::find(child_centers.begin(), child_centers.end(), parent_center) != child_centers.end();
        canvas.put(static_cast<size_t>(parent_center), y, parent_over_child ? "┼" : "┴");
    }

    void drawEdges(TextCanvas & canvas, size_t index)
    {
        const auto & step_box = boxes[index];
        if (step_box.children.empty())
            return;

        const auto center = static_cast<size_t>(step_box.center);
        canvas.put(center, step_box.bottom(), "▲");

        VectorWithMemoryTracking<size_t> in_fragment;
        VectorWithMemoryTracking<size_t> producers;
        for (size_t child : step_box.children)
            (crossesFragments(index, child) ? producers : in_fragment).push_back(child);

        if (!in_fragment.empty())
        {
            canvas.put(center, step_box.bottom() + 1, "│");
            if (in_fragment.size() > 1)
            {
                VectorWithMemoryTracking<Int64> centers;
                for (size_t child : in_fragment)
                    centers.push_back(boxes[child].center);
                drawBranch(canvas, step_box.bottom() + 2, step_box.center, centers);
            }
            for (size_t child : in_fragment)
                canvas.put(static_cast<size_t>(boxes[child].center), boxes[child].y, "┴");
        }

        if (producers.empty())
            return;

        /// From the receive step down to the frame of its fragment, across it, along the labels, across the frame of
        /// the producer, to its send step.
        const auto & frame = frames[step_box.fragment];
        for (size_t y = step_box.bottom() + 1; y < frame.bottom; ++y)
            canvas.put(center, y, "│");
        canvas.put(center, frame.bottom, "┼");

        size_t labels_y = frame.bottom + 1;
        if (producers.size() > 1)
        {
            VectorWithMemoryTracking<Int64> centers;
            for (size_t child : producers)
                centers.push_back(boxes[child].center);
            drawBranch(canvas, labels_y, step_box.center, centers);
            ++labels_y;
        }

        for (size_t producer_root : producers)
        {
            const auto & producer = boxes[producer_root];
            const auto x = static_cast<size_t>(producer.center);
            const auto & exchange = graph.exchanges[*producer.incoming_exchange];
            const char * line = exchange.mode == ExchangeDescription::Kind::Persisted ? "╎" : "│";

            const size_t producer_top = producer.y - 2;
            for (size_t y = labels_y; y < producer_top; ++y)
                canvas.put(x, y, line);
            auto lines = labelLines(*producer.incoming_exchange);
            for (size_t i = 0; i < lines.size(); ++i)
                canvas.put(x + 2, labels_y + i, lines[i]);
            canvas.put(x, producer_top, "┼");
            canvas.put(x, producer_top + 1, line);
            canvas.put(x, producer.y, "┴");
        }
    }
};

}

void explainAsTree(
    const FragmentGraph & graph,
    WriteBuffer & buf,
    const ExplainPlanOptions & options,
    size_t max_step_description_length)
{
    const PreparedFragmentGraph prepared(graph, options);

    validateFragmentGraph(graph);

    /// The result columns of the query, as plain `EXPLAIN` prints them before the plan: no box shows them, because
    /// `Expression` steps are not drawn. The fragment that produces the result (`main`) is the first in `print_order`.
    if (options.pretty)
    {
        const QueryFragment & result_fragment
            = prepared.single_node ? graph.fragments.front() : *prepared.print_order.front();
        const QueryPlan & result_plan = PreparedFragmentGraph::getPlan(result_fragment);
        if (!result_plan.getRootNode())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Fragment {} has no plan to draw", result_fragment.id);
        QueryPlanFormat::formatOutputColumns(
            prepared.getPrettyNames(result_plan).pretty_names, buf, *result_plan.getRootNode()->step, /*prefix=*/ "");
        buf.write('\n');
    }

    NestedPlans nested_plans;
    TreeBuilder(prepared, max_step_description_length, nested_plans, prepared.single_node).drawGraph(buf);

    /// Drawing a nested plan can register the nested plans of its own steps: they are appended and drawn in turn.
    for (size_t i = 0; i < nested_plans.size(); ++i)
    {
        /// Copy: drawing appends to `nested_plans`, which invalidates references to its elements.
        const NestedPlan nested_plan = nested_plans[i];
        buf << "\nN" << i + 1 << " · nested plan of " << TextCanvas::replaceControlCharacters(nested_plan.owner) << '\n';
        TreeBuilder(prepared, max_step_description_length, nested_plans, /*frameless=*/ true)
            .drawNestedPlan(*nested_plan.plan, buf);
    }

    /// A plan that was not cut has no exchanges, and its one level says nothing.
    if (prepared.single_node)
    {
        const auto & fragment = graph.fragments.front();
        if (fragment.fallback_reason)
            buf << "\nNot distributed: " << *fragment.fallback_reason << '\n';
        return;
    }

    buf.write('\n');
    writeExchangesSection(prepared, buf);
    writeLevelsSection(graph, buf);
}

}
