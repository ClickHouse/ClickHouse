#pragma once
#include <array>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>
#include <Core/Types.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <absl/container/flat_hash_map.h>


namespace DB
{
/// Finite State Transducer is an efficient way to represent term dictionary.
/// It can be viewed as a map of <term, output> where output is an integer.
/// Detailed explanation can be found in the following paper
/// [Direct Construction of Minimal Acyclic Subsequential Transduers] by Stoyan Mihov and Denis Maurel, University of Tours, France
namespace FST
{

using Output = UInt64;

class State;
using StatePtr = std::shared_ptr<State>;

/// Arc represents a transition from one state to another.
/// It includes the target state to which the arc points and the arc's output.
struct Arc
{
    Arc() = default;
    Arc(Output output_, const StatePtr & target_);

    /// 0 means the arc has no output
    Output output = 0;

    StatePtr target;

    UInt64 serialize(WriteBuffer & write_buffer) const;
};

bool operator==(const Arc & arc1, const Arc & arc2);

/// LabelsAsBitmap implements a 256-bit bitmap for all labels of a state. Each bit represents
/// a label's presence and the index value of the bit represents the corresponding label
class LabelsAsBitmap
{
public:
    void addLabel(char label);
    bool hasLabel(char label) const;

    /// computes the rank
    UInt64 getIndex(char label) const;

    UInt64 serialize(WriteBuffer & write_buffer);

    UInt64 deserialize(ReadBuffer & read_buffer);
private:
    /// data holds a 256-bit bitmap for all labels of a state. Its 256 bits correspond to 256
    /// possible label values.
    UInt256 data = 0;

    friend class State;
    friend class FiniteStateTransducer;
};

/// State implements the State in Finite State Transducer
/// Each state contains all its arcs and a flag indicating if it is final state
class State
{
public:
    static constexpr size_t MAX_ARCS_IN_SEQUENTIAL_METHOD = 32;
    enum class EncodingMethod : uint8_t
    {
        /// Serialize arcs sequentially
        Sequential = 0,

        /// Serialize arcs by using bitmap
        /// Note this is NOT enabled for now since it is experimental
        Bitmap,
    };

    State() = default;
    State(const State & State) = default;

    UInt64 hash() const;

    Arc * getArc(char label) const;

    void addArc(char label, Output output, StatePtr target);

    void clear();

    UInt64 serialize(WriteBuffer & write_buffer);

    bool isFinal() const { return flag_values.is_final == 1; }
    void setFinal(bool value) { flag_values.is_final = value; }

    EncodingMethod getEncodingMethod() const { return flag_values.encoding_method; }

    void readFlag(ReadBuffer & read_buffer);

    /// Transient ID of the state which is used for building FST. It won't be serialized
    UInt64 id = 0;

    /// State index which indicates location of state in FST
    UInt64 state_index = 0;

    /// Arcs which are started from state, the 'char' is the label on the arc
    absl::flat_hash_map<char, Arc> arcs;

private:
    struct FlagValues
    {
        uint8_t is_final : 1;
        EncodingMethod encoding_method : 3;
    };

    union
    {
        FlagValues flag_values;
        uint8_t flag = 0;
    };
};

bool operator==(const State & state1, const State & state2);

static constexpr size_t MAX_TERM_LENGTH = 256;

/// FstBuilder is used to build Finite State Transducer by adding words incrementally.
/// Note that all the words have to be added in sorted order in order to achieve minimized result.
/// In the end, the caller should call build() to serialize minimized FST to WriteBuffer.
class FstBuilder
{
public:
    explicit FstBuilder(WriteBuffer & write_buffer_);

    void add(std::string_view word, Output output);
    UInt64 build();
private:
    StatePtr findMinimized(const State & s, bool & found);
    void minimizePreviousWordSuffix(Int64 down_to);

    std::array<StatePtr, MAX_TERM_LENGTH + 1> temp_states;
    String previous_word;
    StatePtr initial_state;

    /// map of (state_hash, StatePtr)
    absl::flat_hash_map<UInt64, StatePtr> minimized_states;

    /// Next available ID of state
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
    explicit FiniteStateTransducer(std::vector<UInt8> data_);
    void clear();
    std::pair<UInt64, bool> getOutput(std::string_view term);
    std::vector<UInt8> & getData() { return data; }

private:
    std::vector<UInt8> data;
};
}
}
