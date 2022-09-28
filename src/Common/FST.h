#pragma once
#include <array>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>

namespace DB
{
/// Finite State Transducer is an efficient way to represent term dictionary.
/// It can be viewed as a map of <term, offset>.
/// Detailed explanation can be found in the following paper
/// [Direct Construction of Minimal Acyclic Subsequential Transduers] by Stoyan Mihov and Denis Maurel, University of Tours, France
namespace FST
{
    using Output = UInt64;

    class State;
    using StatePtr = std::shared_ptr<State>;

    /// Arc represents a transition from one state to another
    /// It includes the target state to which the arc points and its output.
    struct Arc
    {
        Arc() = default;
        explicit Arc(Output output_, const StatePtr & target_) : output{output_}, target{target_} { }
        Output output = 0;
        StatePtr target;

        UInt64 serialize(WriteBuffer & write_buffer) const;
    };

    bool operator==(const Arc & arc1, const Arc & arc2);

    /// ArcsAsBitmap implements a 256-bit bitmap for all arcs of a state. Each bit represents
    /// an arc's presence and the index value of the bit represents the corresponding label
    class ArcsAsBitmap
    {
    public:
        void addArc(char label);
        bool hasArc(char label) const;
        int getIndex(char label) const;

    private:
        friend class State;
        friend class FiniteStateTransducer;
        /// data holds a 256-bit bitmap for all arcs of a state. Ita 256 bits correspond to 256
        /// possible label values.
        UInt256 data{ 0 };
    };

    /// State implements the State in Finite State Transducer
    /// Each state contains all its arcs and a flag indicating if it is final state
    class State
    {
    public:
        static constexpr size_t MAX_ARCS_IN_SEQUENTIAL_METHOD = 32;
        enum class EncodingMethod
        {
            ENCODING_METHOD_SEQUENTIAL = 0,
            ENCODING_METHOD_BITMAP,
        };
        State() = default;
        State(const State & state) = default;

        UInt64 hash() const;

        Arc * getArc(char label);
        void addArc(char label, Output output, StatePtr target);

        void clear()
        {
            id = 0;
            state_index = 0;
            flag = 0;

            arcs.clear();
        }

        UInt64 serialize(WriteBuffer & write_buffer);

        inline bool isFinal() const
        {
            return flag_values.is_final == 1;
        }
        inline void setFinal(bool value)
        {
            flag_values.is_final = value;
        }
        inline EncodingMethod getEncodingMethod() const
        {
            return flag_values.encoding_method;
        }
        inline void readFlag(ReadBuffer & read_buffer)
        {
            read_buffer.read(reinterpret_cast<char&>(flag));
        }

        UInt64 id = 0;
        UInt64 state_index = 0;
        std::unordered_map<char, Arc> arcs;
    private:
        struct FlagValues
        {
            unsigned int is_final : 1;
            EncodingMethod encoding_method : 3;
        };

        union
        {
            FlagValues flag_values;
            uint8_t flag = 0;
        };
    };

    bool operator==(const State & state1, const State & state2);

    inline constexpr size_t MAX_TERM_LENGTH = 256;

    /// FSTBuilder is used to build Finite State Transducer by adding words incrementally.
    /// Note that all the words have to be added in sorted order in order to achieve minimized result.
    /// In the end, the caller should call build() to serialize minimized FST to WriteBuffer
    class FSTBuilder
    {
    public:
        FSTBuilder(WriteBuffer & write_buffer_);
        StatePtr getMinimized(const State & s, bool & found);

        void add(const std::string & word, Output output);
        UInt64 build();

        UInt64 state_count = 0;

    private:
        void minimizePreviousWordSuffix(Int64 down_to);
        static size_t getCommonPrefix(const std::string & word1, const std::string & word2);

        std::array<StatePtr, MAX_TERM_LENGTH + 1> temp_states;
        std::string previous_word;
        StatePtr initial_state;
        std::unordered_map<UInt64, StatePtr> minimized_states;

        UInt64 next_id = 1;

        WriteBuffer & write_buffer;
        UInt64 previous_written_bytes = 0;
        UInt64 previous_state_index = 0;
    };

    //FiniteStateTransducer is constructed by using minimized FST blob(which is loaded from index storage)
    // It is used to retrieve output by given term
    class FiniteStateTransducer
    {
    public:
        FiniteStateTransducer() = default;
        FiniteStateTransducer(std::vector<UInt8> data_);
        std::pair<UInt64, bool> getOutput(const String & term);
        void clear();
        std::vector<UInt8> & getData() { return data; }

    private:
        std::vector<UInt8> data;
    };
}
}
