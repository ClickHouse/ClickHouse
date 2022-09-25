#include "FST.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <memory>
#include <vector>
#include <Common/Exception.h>
#include <Common/HashTable/Hash.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

namespace FST
{

UInt64 Arc::serialize(WriteBuffer& write_buffer) const
{
    UInt64 written_bytes = 0;
    bool has_output = output != 0;

    /// First UInt64 is target_index << 1 + has_output
    assert(target != nullptr);
    UInt64 first = ((target->state_index) << 1) + has_output;
    writeVarUInt(first, write_buffer);
    written_bytes += getLengthOfVarUInt(first);

    /// Second UInt64 is output (optional based on whether has_output is not zero)
    if (has_output)
    {
        writeVarUInt(output, write_buffer);
        written_bytes += getLengthOfVarUInt(output);
    }
    return written_bytes;
}

bool operator==(const Arc & arc1, const Arc & arc2)
{
    return (arc1.output == arc2.output && arc1.target->id == arc2.target->id);
}

void ArcsAsBitmap::addArc(char label)
{
    UInt8 index = label;
    UInt256 bit_label = 1;
    bit_label <<= index;

    data |= bit_label;
}

int ArcsAsBitmap::getIndex(char label) const
{
    int bit_count = 0;

    UInt8 index = label;
    int which_int64 = 0;
    while (true)
    {
        if (index < 64)
        {
            UInt64 mask = index == 63 ? (-1) : (1ULL << (index + 1)) - 1;

            bit_count += std::popcount(mask & data.items[which_int64]);
            break;
        }
        index -= 64;
        bit_count += std::popcount(data.items[which_int64]);

        which_int64++;
    }
    return bit_count;
}

bool ArcsAsBitmap::hasArc(char label) const
{
    UInt8 index = label;
    UInt256 bit_label = 1;
    bit_label <<= index;

    return ((data & bit_label) != 0);
}

Arc* State::getArc(char label)
{
    auto it = arcs.find(label);
    if (it == arcs.cend())
        return nullptr;

    return &it->second;
}

void State::addArc(char label, Output output, StatePtr target)
{
    arcs[label] = Arc(output, target);
}

UInt64 State::hash() const
{
    std::vector<char> values;
    values.reserve(arcs.size() * (sizeof(Output) + sizeof(UInt64) + 1));
    for (const auto & [label, arc] : arcs)
    {
        values.push_back(label);
        const auto * ptr = reinterpret_cast<const char*>(&arc.output);
        std::copy(ptr, ptr + sizeof(Output), std::back_inserter(values));

        ptr = reinterpret_cast<const char*>(&arc.target->id);
        std::copy(ptr, ptr + sizeof(UInt64), std::back_inserter(values));
    }

    return CityHash_v1_0_2::CityHash64(values.data(), values.size());
}

bool operator== (const State& state1, const State& state2)
{
    if (state1.arcs.size() != state2.arcs.size())
        return false;

    for (const auto & [label, arc] : state1.arcs)
    {
        const auto it = state2.arcs.find(label);
        if (it == state2.arcs.cend())
            return false;

        if (it->second != arc)
            return false;
    }
    return true;
}

UInt64 State::serialize(WriteBuffer& write_buffer)
{
    UInt64 written_bytes = 0;

    /// Serialize flag
    write_buffer.write(flag);
    written_bytes += 1;

    if (flag_values.encoding_method == EncodingMethod::ENCODING_METHOD_SEQUENTIAL)
    {
        /// Serialize all labels
        std::vector<char> labels;
        labels.reserve(MAX_ARCS_IN_SEQUENTIAL_METHOD);

        for (auto& [label, state] : arcs)
        {
            labels.push_back(label);
        }

        UInt8 label_size = labels.size();
        write_buffer.write(label_size);
        written_bytes += 1;

        write_buffer.write(labels.data(), labels.size());
        written_bytes += labels.size();

        /// Serialize all arcs
        for (char label : labels)
        {
            Arc* arc = getArc(label);
            assert(arc != nullptr);
            written_bytes += arc->serialize(write_buffer);
        }
    }
    else
    {
        /// Serialize bitmap
        ArcsAsBitmap bmp;
        for (auto & [label, state] : arcs)
        {
            bmp.addArc(label);
        }

        UInt64 bmp_encoded_size = getLengthOfVarUInt(bmp.data.items[0])
            + getLengthOfVarUInt(bmp.data.items[1])
            + getLengthOfVarUInt(bmp.data.items[2])
            + getLengthOfVarUInt(bmp.data.items[3]);

        writeVarUInt(bmp.data.items[0], write_buffer);
        writeVarUInt(bmp.data.items[1], write_buffer);
        writeVarUInt(bmp.data.items[2], write_buffer);
        writeVarUInt(bmp.data.items[3], write_buffer);

        written_bytes += bmp_encoded_size;
        /// Serialize all arcs
        for (auto & [label, state] : arcs)
        {
            Arc* arc = getArc(label);
            assert(arc != nullptr);
            written_bytes += arc->serialize(write_buffer);
        }
    }

    return written_bytes;
}

FSTBuilder::FSTBuilder(WriteBuffer& write_buffer_) : write_buffer(write_buffer_)
{
    for (auto & temp_state : temp_states)
    {
        temp_state = std::make_shared<State>();
    }
}

StatePtr FSTBuilder::getMinimized(const State& state, bool& found)
{
    found = false;
    auto hash = state.hash();
    auto it = minimized_states.find(hash);

    if (it != minimized_states.cend() && *it->second == state)
    {
        found = true;
        return it->second;
    }
    StatePtr p = std::make_shared<State>(state);
    minimized_states[hash] = p;
    return p;
}

size_t FSTBuilder::getCommonPrefix(const std::string& word1, const std::string& word2)
{
    size_t i = 0;
    while (i < word1.size() && i < word2.size() && word1[i] == word2[i])
        i++;
    return i;
}

void FSTBuilder::minimizePreviousWordSuffix(Int64 down_to)
{
    for (Int64 i = static_cast<Int64>(previous_word.size()); i >= down_to; --i)
    {
        bool found{ false };
        auto minimized_state = getMinimized(*temp_states[i], found);

        if (i != 0)
        {
            Output output = 0;
            Arc* arc = temp_states[i - 1]->getArc(previous_word[i - 1]);
            if (arc)
                output = arc->output;

            temp_states[i - 1]->addArc(previous_word[i - 1], output, minimized_state);
        }
        if (minimized_state->id == 0)
            minimized_state->id = next_id++;

        if (i > 0 && temp_states[i - 1]->id == 0)
            temp_states[i - 1]->id = next_id++;

        if (!found)
        {
            minimized_state->state_index = previous_state_index;

            previous_written_bytes = minimized_state->serialize(write_buffer);
            state_count++;
            previous_state_index += previous_written_bytes;
        }
    }
}

void FSTBuilder::add(const std::string& current_word, Output current_output)
{
    /// We assume word size is no greater than MAX_TERM_LENGTH(256).
    /// FSTs without word size limitation would be inefficient and easy to cause memory bloat
    /// Note that when using "split" tokenizer, if a granule has tokens which are longer than
    /// MAX_TERM_LENGTH, the granule cannot be dropped and will be fully-scanned. It doesn't affect "ngram" tokenizers.
    /// Another limitation is that if the query string has tokens which exceed this length
    /// it will fallback to default searching when using "split" tokenizers.
    auto current_word_len = current_word.size();

    if (current_word_len > MAX_TERM_LENGTH)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too long term ({}) passed to FST builder.", current_word_len);

    size_t prefix_length_plus1 = getCommonPrefix(current_word, previous_word) + 1;

    minimizePreviousWordSuffix(prefix_length_plus1);

    /// Initialize the tail state
    for (size_t  i = prefix_length_plus1; i <= current_word.size(); ++i)
    {
        temp_states[i]->clear();
        temp_states[i - 1]->addArc(current_word[i - 1], 0, temp_states[i]);
    }

    /// We assume the current word is different with previous word
    temp_states[current_word_len]->setFinal(true);
    /// Adjust outputs on the arcs
    for (size_t j = 1; j <= prefix_length_plus1 - 1; ++j)
    {
        Arc * arc_ptr = temp_states[j - 1]->getArc(current_word[j - 1]);
        assert(arc_ptr != nullptr);

        Output common_prefix = std::min(arc_ptr->output, current_output);
        Output word_suffix = arc_ptr->output - common_prefix;
        arc_ptr->output = common_prefix;

        /// For each arc, adjust its output
        if (word_suffix != 0)
        {
            for (auto & [label, arc] : temp_states[j]->arcs)
            {
                arc.output += word_suffix;
            }
        }
        /// Reduce current_output
        current_output -= common_prefix;
    }

    /// Set last temp state's output
    Arc * arc = temp_states[prefix_length_plus1 - 1]->getArc(current_word[prefix_length_plus1 - 1]);
    assert(arc != nullptr);
    arc->output = current_output;

    previous_word = current_word;
}

UInt64 FSTBuilder::build()
{
    minimizePreviousWordSuffix(0);

    /// Save initial state index

    previous_state_index -= previous_written_bytes;
    UInt8 length = getLengthOfVarUInt(previous_state_index);
    writeVarUInt(previous_state_index, write_buffer);
    write_buffer.write(length);

    return previous_state_index + previous_written_bytes + length + 1;
}

FiniteStateTransducer::FiniteStateTransducer(std::vector<UInt8> data_) : data(std::move(data_))
{
}

void FiniteStateTransducer::clear()
{
    data.clear();
}

std::pair<UInt64, bool> FiniteStateTransducer::getOutput(const String& term)
{
    std::pair<UInt64, bool> result_output{ 0, false };
    /// Read index of initial state
    ReadBufferFromMemory read_buffer(data.data(), data.size());
    read_buffer.seek(data.size()-1, SEEK_SET);

    UInt8 length{ 0 };
    read_buffer.read(reinterpret_cast<char&>(length));
    if (length == 0)
        return { 0, false };

    read_buffer.seek(data.size() - 1 - length, SEEK_SET);
    UInt64 state_index{ 0 };
    readVarUInt(state_index, read_buffer);

    for (size_t i = 0; i <= term.size(); ++i)
    {
        UInt64 arc_output{ 0 };

        /// Read flag
        State temp_state;

        read_buffer.seek(state_index, SEEK_SET);
        temp_state.readFlag(read_buffer);
        if (i == term.size())
        {
            result_output.second = temp_state.isFinal();
            break;
        }

        UInt8 label = term[i];
        if (temp_state.getEncodingMethod() == State::EncodingMethod::ENCODING_METHOD_SEQUENTIAL)
        {
            /// Read number of labels
            UInt8 label_num{ 0 };
            read_buffer.read(reinterpret_cast<char&>(label_num));

            if (label_num == 0)
                return { 0, false };

            auto labels_position = read_buffer.getPosition();

            /// Find the index of the label from "labels" bytes
            auto begin_it{ data.begin() + labels_position };
            auto end_it{ data.begin() + labels_position + label_num };

            auto pos = std::find(begin_it, end_it, label);

            if (pos == end_it)
                return { 0, false };

            /// Read the arc for the label
            UInt64 arc_index = (pos - begin_it);
            auto arcs_start_postion = labels_position + label_num;

            read_buffer.seek(arcs_start_postion, SEEK_SET);
            for (size_t j = 0; j <= arc_index; j++)
            {
                state_index = 0;
                arc_output = 0;
                readVarUInt(state_index, read_buffer);
                if (state_index & 0x1) // output is followed
                {
                    readVarUInt(arc_output, read_buffer);
                }
                state_index >>= 1;
            }
        }
        else
        {
            ArcsAsBitmap bmp;

            readVarUInt(bmp.data.items[0], read_buffer);
            readVarUInt(bmp.data.items[1], read_buffer);
            readVarUInt(bmp.data.items[2], read_buffer);
            readVarUInt(bmp.data.items[3], read_buffer);

            if (!bmp.hasArc(label))
                return { 0, false };

            /// Read the arc for the label
            size_t arc_index = bmp.getIndex(label);
            for (size_t j = 0; j < arc_index; j++)
            {
                state_index = 0;
                arc_output = 0;
                readVarUInt(state_index, read_buffer);
                if (state_index & 0x1) // output is followed
                {
                    readVarUInt(arc_output, read_buffer);
                }
                state_index >>= 1;
            }
        }
        /// Accumulate the output value
        result_output.first += arc_output;
    }
    return result_output;
}
}
}
