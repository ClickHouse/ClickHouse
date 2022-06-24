#include <cassert>
#include <iostream>
#include <memory>
#include <vector>
#include <algorithm>
#include <../contrib/consistent-hashing/popcount.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Common/HashTable/Hash.h>
#include <Common/Exception.h>
#include "FST.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

UInt16 Arc::serialize(WriteBuffer& write_buffer)
{
    UInt16 written_bytes = 0;
    bool has_output = output != 0;

    /// First UInt64 is target_index << 1 + has_output
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

void ArcsBitmap::addArc(char label)
{
    uint8_t index = label;
    UInt256 bit_label = 1;
    bit_label <<= index;

    data |= bit_label;
}

int ArcsBitmap::getIndex(char label) const
{		
    int bitCount = 0;

    uint8_t index = label;
    int which_int64 = 0;
    while (true)
    {
        if (index < 64)
        {
            UInt64 mask = index == 63 ? (-1) : (1ULL << (index+1)) - 1;

            bitCount += PopCountImpl(mask & data.items[which_int64]);
            break;
        }
        index -= 64;
        bitCount += PopCountImpl(data.items[which_int64]);

        which_int64++;
    }
    return bitCount;
}

int ArcsBitmap::getArcNum() const
{
    int bitCount = 0;
    for (size_t i = 0; i < 4; i++)
    {
        if(data.items[i])
            bitCount += PopCountImpl(data.items[i]);
    }
    return bitCount;
}

bool ArcsBitmap::hasArc(char label) const
{
    uint8_t index = label;
    UInt256 bit_label = 1;
    bit_label <<= index;

    return ((data & bit_label) != 0);
}

Arc* State::getArc(char label)
{
    auto it(arcs.find(label));
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

    // put 2 magic chars, in case there are no arcs
    values.push_back('C');
    values.push_back('H');

    for (auto& label_arc : arcs)
    {
        values.push_back(label_arc.first);
        auto ptr = reinterpret_cast<const char*>(&label_arc.second.output);
        std::copy(ptr, ptr + sizeof(Output), std::back_inserter(values));

        ptr = reinterpret_cast<const char*>(&label_arc.second.target->id);
        std::copy(ptr, ptr + sizeof(UInt64), std::back_inserter(values));
    }

    return CityHash_v1_0_2::CityHash64(values.data(), values.size());
}

bool operator== (const State& state1, const State& state2)
{
    for (auto& label_arc : state1.arcs)
    {
        auto it(state2.arcs.find(label_arc.first));
        if (it == state2.arcs.cend())
            return false;
        if (it->second.output != label_arc.second.output)
            return false;
        if (it->second.target->id != label_arc.second.target->id)
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

    if (flag_values.encoding_method == ENCODING_METHOD_SEQUENTIAL)
    {
        /// Serialize all labels
        std::vector<char> labels;
        labels.reserve(MAX_ARCS_IN_SEQUENTIAL_METHID);

        for (auto& label_state : arcs)
        {
            labels.push_back(label_state.first);
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
        ArcsBitmap bmp;
        for (auto& label_state : arcs)
        {
            bmp.addArc(label_state.first);
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
        for (auto& label_state : arcs)
        {
            Arc* arc = getArc(label_state.first);
            assert(arc != nullptr);
            written_bytes += arc->serialize(write_buffer);
            }
    }

    return written_bytes;
}

FSTBuilder::FSTBuilder(WriteBuffer& write_buffer_) : write_buffer(write_buffer_)
{
    for (size_t i = 0; i < temp_states.size(); ++i)
    {
        temp_states[i] = std::make_shared<State>();
    }
}

StatePtr FSTBuilder::findMinimized(const State& state, bool& found)
{
    found = false;
    auto hash = state.hash();
    auto it(minimized_states.find(hash));

    if (it != minimized_states.cend() && *it->second == state)
    {
        found = true;
        return it->second;
    }
    StatePtr p(new State(state));
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

void FSTBuilder::minimizePreviousWordSuffix(int down_to)
{
    for (int i = static_cast<int>(previous_word.size()); i >= down_to; --i)
    {
        bool found{ false };
        auto minimized_state = findMinimized(*temp_states[i], found);

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
    /// We assume word size is no greater than MAX_TERM_LENGTH
    auto current_word_len = current_word.size();

    if (current_word_len > MAX_TERM_LENGTH)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too long term ({}) passed to FST builder.", current_word_len);

    size_t prefix_length_plus1 = getCommonPrefix(current_word, previous_word) + 1;

    minimizePreviousWordSuffix(prefix_length_plus1);

    /// Initialize the tail state
    for (size_t  i = prefix_length_plus1; i <= current_word.size(); ++i)
    {
        temp_states[i]->clear();
        temp_states[i - 1]->addArc(current_word[i-1], 0, temp_states[i]);
    }

    /// We assume the current word is different with previous word
    temp_states[current_word_len]->flag_values.is_final = true;
    /// Adjust outputs on the arcs
    for (size_t j = 1; j <= prefix_length_plus1 - 1; ++j)
    {
        auto arc_ptr = temp_states[j - 1]->getArc(current_word[j-1]);
        assert(arc_ptr != nullptr);

        auto common_prefix = std::min(arc_ptr->output, current_output);
        auto word_suffix = arc_ptr->output - common_prefix;
        arc_ptr->output = common_prefix;

        /// For each arc, adjust its output
        if (word_suffix != 0)
        {
            for (auto& label_arc : temp_states[j]->arcs)
            {
                auto& arc = label_arc.second;
                arc.output += word_suffix;
            }
        }
        /// Reduce current_output
        current_output -= common_prefix;
    }

    /// Set last temp state's output
    auto arc = temp_states[prefix_length_plus1 - 1]->getArc(current_word[prefix_length_plus1-1]);
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

FST::FST(std::vector<UInt8>&& data_) : data(data_)
{
}

void FST::clear()
{
    data.clear();
}

std::pair<bool, UInt64> FST::getOutput(const String& term)
{
    std::pair<bool, UInt64> result_output{ false, 0 };
    /// Read index of initial state
    ReadBufferFromMemory read_buffer(data.data(), data.size());
    read_buffer.seek(data.size()-1, SEEK_SET);

    UInt8 length{0};
    read_buffer.read(reinterpret_cast<char&>(length));

    read_buffer.seek(data.size() - 1 - length, SEEK_SET);
    UInt64 state_index{ 0 };
    readVarUInt(state_index, read_buffer);

    for (size_t i = 0; i <= term.size(); ++i)
    {
        UInt64 arc_output{ 0 };

        /// Read flag
        State temp_state;

        read_buffer.seek(state_index, SEEK_SET);
        read_buffer.read(reinterpret_cast<char&>(temp_state.flag));
        if (i == term.size())
        {
            result_output.first = temp_state.flag_values.is_final;
            break;
        }

        UInt8 label = term[i];
        if (temp_state.flag_values.encoding_method == State::ENCODING_METHOD_SEQUENTIAL)
        {
            /// Read number of labels
            UInt8 label_num{0};
            read_buffer.read(reinterpret_cast<char&>(label_num));

            if(label_num == 0)
                return { false, 0 };

            auto labels_position = read_buffer.getPosition();

            /// Find the index of the label from "labels" bytes
            auto begin_it{ data.begin() + labels_position };
            auto end_it{ data.begin() + labels_position + label_num };

            auto pos = std::find(begin_it, end_it, label);

            if (pos == end_it)
                return { false, 0 };

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
            ArcsBitmap bmp;

            readVarUInt(bmp.data.items[0], read_buffer);
            readVarUInt(bmp.data.items[1], read_buffer);
            readVarUInt(bmp.data.items[2], read_buffer);
            readVarUInt(bmp.data.items[3], read_buffer);

            if (!bmp.hasArc(label))
                return { false, 0 };

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
        result_output.second += arc_output;
    }
    return result_output;
}
}
