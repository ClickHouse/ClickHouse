#pragma once
#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <map>
#include <memory>
#include <array>
#include <Core/Types.h>
#include <base/types.h>
#include <IO/WriteBuffer.h>

namespace DB
{
using Output = UInt64;

struct State;
using StatePtr = std::shared_ptr<State>;

struct Arc
{
    Arc() = default;
    Arc(Output output_, const StatePtr &target_): output{output_}, target{target_}{}
    Output		output{0};
    StatePtr	target;

    UInt16 serialize(WriteBuffer& write_buffer);
};

class ArcsBitmap
{
public:
    void addArc(char label);
    bool hasArc(char label) const;
    int getIndex(char label) const;
    int getArcNum() const;
private:
    friend struct State;
    friend class FST;
    UInt256 data{0};
};

struct State
{
    static constexpr size_t MAX_ARCS_IN_SEQUENTIAL_METHID = 32;
    enum EncodingMethod
    {
        ENCODING_METHOD_SEQUENTIAL = 0,
        ENCODING_METHOD_BITMAP,
    };
    State() = default;
    State(const State &state) = default;

    UInt64 hash() const;

    Arc* getArc(char label);
    void addArc(char label, Output output, StatePtr target);

    void clear()
    {
        id = 0;
        state_index = 0;
        flag = 0;

        arcs.clear();
    }

    UInt64 serialize(WriteBuffer& write_buffer);

    UInt64 id{ 0 };
    UInt64 state_index{ 0 };
    union
    {
        struct
        {
            unsigned int is_final : 1;
            unsigned int encoding_method : 2;
        } flag_values;
        uint8_t flag{0};
    };
    std::map<char, Arc> arcs;
};

bool operator== (const State& state1, const State& state2);

constexpr size_t MAX_TERM_LENGTH = 256;

class FSTBuilder
{
public:
    FSTBuilder(WriteBuffer& write_buffer_);
    StatePtr findMinimized(const State& s, bool &found);

    void add(const std::string& word, Output output);
    UInt64 build();

    UInt64 state_count{ 0 };
private:
    void minimizePreviousWordSuffix(int down_to);
    static size_t getCommonPrefix(const std::string &word1, const std::string& word2);
private:
    std::array<StatePtr, MAX_TERM_LENGTH + 1> temp_states;
    std::string previous_word;
    StatePtr initial_state;
    std::unordered_map <UInt64, StatePtr> minimized_states;

    UInt64 next_id{1};

    WriteBuffer &write_buffer;
    UInt64 previous_written_bytes{0};
    UInt64 previous_state_index{0};

};

class FST
{
public:
    FST() = default;
    FST(std::vector<UInt8>&& data_);
    std::pair<bool, UInt64> getOutput(const String& term);
    void clear();
    std::vector<UInt8> &getData() { return data;}
private:
    std::vector<UInt8> data;
};
}
