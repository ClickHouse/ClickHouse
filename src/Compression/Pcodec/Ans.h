#pragma once

#include <Compression/Pcodec/PcoArray.h>

#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/PcodecError.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <utility>
#include <vector>

/** Table-based interleaved asymmetric numeral system (tANS) coder, ported from
  * tmp/pcodec_ref/pco/src/ans/{spec.rs,decoding.rs}.
  *
  * The symbol spreading (`spreadStateSymbols`) is part of the wire format and MUST stay backward
  * compatible / bit-exact: it deterministically assigns each ANS table state to a symbol.
  */
namespace DB::Pcodec
{

/// We use a relatively prime (odd) number near 3/5 of the table size, which spreads uncommon
/// symbols reasonably. Matches `ans::spec::choose_stride`.
inline Weight chooseStride(Weight table_size)
{
    Weight res = (3 * table_size) / 5;
    if (res % 2 == 0)
        res += 1;
    return res;
}

struct AnsSpec
{
    /// log2 of the table size; table states are in [2^size_log, 2^(size_log+1)).
    Bitlen size_log = 0;
    /// The symbol assigned to each table state, in state order.
    PcoArray<Symbol> state_symbols;
    /// How many times each symbol appears in the table.
    PcoArray<Weight> symbol_weights;

    size_t tableSize() const { return size_t{1} << size_log; }

    /// Deterministically spread symbols across the table. Backward-compatible; do not change.
    static PcoArray<Symbol> spreadStateSymbols(Bitlen size_log, const PcoArray<Weight> & symbol_weights)
    {
        Weight table_size = std::accumulate(symbol_weights.begin(), symbol_weights.end(), Weight{0});
        if (table_size != (Weight{1} << size_log))
            throw PcodecError("pcodec: ANS table size log does not agree with total symbol weight");

        PcoArray<Symbol> res(table_size, 0);
        Weight step = 0;
        Weight stride = chooseStride(table_size);
        Weight mod_table_size = (std::numeric_limits<Weight>::max() >> 1) >> (32 - 1 - size_log);
        for (Symbol symbol = 0; symbol < symbol_weights.size(); ++symbol)
        {
            for (Weight i = 0; i < symbol_weights[symbol]; ++i)
            {
                Weight state_idx = (stride * step) & mod_table_size;
                res[state_idx] = symbol;
                ++step;
            }
        }
        return res;
    }

    static AnsSpec fromWeights(Bitlen size_log, PcoArray<Weight> symbol_weights)
    {
        if (symbol_weights.empty())
            symbol_weights = {1};
        AnsSpec spec;
        spec.size_log = size_log;
        spec.symbol_weights = symbol_weights;
        spec.state_symbols = spreadStateSymbols(size_log, symbol_weights);
        return spec;
    }
};

/// Packed decoder node. The bin's `offset_bits` is folded in as a performance hack (it isn't part
/// of ANS coding itself). Matches `ans::decoding::Node`.
struct AnsNode
{
    uint16_t next_state_idx_base;
    uint8_t offset_bits;
    uint8_t bits_to_read;
};

struct AnsDecoder
{
    PcoArray<AnsNode> nodes;

    /// Builds the decode table. `bin_offset_bits[symbol]` is the offset-bit count for each bin;
    /// a missing entry (degenerate 0-bin case) uses 0. Matches `ans::decoding::Decoder::new`.
    static AnsDecoder make(const AnsSpec & spec, const PcoArray<Bitlen> & bin_offset_bits)
    {
        size_t table_size = spec.tableSize();
        AnsDecoder decoder;
        decoder.nodes.reserve(table_size);
        PcoArray<Weight> symbol_x_s = spec.symbol_weights;
        for (Symbol symbol : spec.state_symbols)
        {
            AnsState next_state_base = symbol_x_s[symbol];
            auto bits_to_read = static_cast<Bitlen>(
                std::countl_zero(next_state_base) - std::countl_zero(static_cast<AnsState>(table_size)));
            next_state_base <<= bits_to_read;
            Bitlen offset_bits = symbol < bin_offset_bits.size() ? bin_offset_bits[symbol] : 0;
            decoder.nodes.push_back(AnsNode{
                static_cast<uint16_t>(next_state_base - static_cast<AnsState>(table_size)),
                static_cast<uint8_t>(offset_bits),
                static_cast<uint8_t>(bits_to_read)});
            symbol_x_s[symbol] += 1;
        }
        return decoder;
    }
};

/// tANS encoder (ans/encoding.rs::Encoder). Operates LIFO (encode in reverse, write forward).
struct AnsEncoder
{
    /// Per-symbol info, kept small (AoS, one cache line per few symbols). `next_states_base` is an
    /// index into the single flat `next_states` array, pre-adjusted by -weight so that the next
    /// state is `next_states[next_states_base + (state >> renorm_bits)]`.
    struct SymbolInfo
    {
        AnsState renorm_bit_cutoff = 0;
        Bitlen min_renorm_bits = 0;
        int64_t next_states_base = 0;
    };

    PcoArray<SymbolInfo> symbol_infos;
    PcoArray<AnsState> next_states; // flat over all symbols (size == table_size)
    Bitlen size_log = 0;

    static AnsEncoder make(const AnsSpec & spec)
    {
        AnsEncoder enc;
        enc.size_log = spec.size_log;
        size_t table_size = spec.tableSize();
        size_t n_symbols = spec.symbol_weights.size();
        enc.symbol_infos.resize(n_symbols);
        enc.next_states.resize(table_size);

        // Contiguous block per symbol: base[s] = sum of weights of earlier symbols.
        PcoArray<size_t> base(n_symbols);
        size_t acc = 0;
        for (size_t s = 0; s < n_symbols; ++s)
        {
            Weight weight = spec.symbol_weights[s];
            Weight max_x_s = 2 * weight - 1;
            Bitlen min_renorm_bits = spec.size_log - static_cast<Bitlen>(31 - std::countl_zero(max_x_s));
            enc.symbol_infos[s].renorm_bit_cutoff = static_cast<AnsState>(2 * weight * (AnsState{1} << min_renorm_bits));
            enc.symbol_infos[s].min_renorm_bits = min_renorm_bits;
            enc.symbol_infos[s].next_states_base = static_cast<int64_t>(acc) - static_cast<int64_t>(weight);
            base[s] = acc;
            acc += weight;
        }
        PcoArray<size_t> filled(n_symbols, 0);
        for (size_t state_idx = 0; state_idx < spec.state_symbols.size(); ++state_idx)
        {
            Symbol s = spec.state_symbols[state_idx];
            enc.next_states[base[s] + filled[s]] = static_cast<AnsState>(table_size + state_idx);
            ++filled[s];
        }
        return enc;
    }

    /// Returns (new_state, number_of_low_bits_of_old_state_to_write).
    std::pair<AnsState, Bitlen> encode(AnsState state, Symbol symbol) const
    {
        const SymbolInfo & info = symbol_infos[symbol];
        Bitlen renorm_bits = state >= info.renorm_bit_cutoff ? info.min_renorm_bits + 1 : info.min_renorm_bits;
        AnsState x_s = state >> renorm_bits;
        return {next_states[static_cast<size_t>(info.next_states_base + static_cast<int64_t>(x_s))], renorm_bits};
    }

    AnsState defaultState() const { return AnsState{1} << size_log; }
};

/// Quantize bin counts to ANS weights that sum to a power of 2 (ans/encoding.rs::quantize_weights_to).
inline PcoArray<Weight> quantizeWeightsTo(const PcoArray<Weight> & counts, size_t total_count, Bitlen size_log)
{
    if (size_log == 0)
        return {1};

    Weight required_weight_sum = Weight{1} << size_log;
    float multiplier = static_cast<float>(required_weight_sum) / static_cast<float>(total_count);
    PcoArray<float> desired_surplus_per_bin(counts.size());
    float desired_surplus = 0;
    for (size_t i = 0; i < counts.size(); ++i)
    {
        desired_surplus_per_bin[i] = std::max(0.0f, static_cast<float>(counts[i]) * multiplier - 1.0f);
        desired_surplus += desired_surplus_per_bin[i];
    }
    Weight required_surplus = required_weight_sum - static_cast<Weight>(counts.size());
    float surplus_mult = desired_surplus == 0.0f ? 0.0f : static_cast<float>(required_surplus) / desired_surplus;

    PcoArray<float> float_weights(counts.size());
    PcoArray<Weight> weights(counts.size());
    Weight weight_sum = 0;
    for (size_t i = 0; i < counts.size(); ++i)
    {
        float_weights[i] = 1.0f + desired_surplus_per_bin[i] * surplus_mult;
        weights[i] = static_cast<Weight>(std::lround(float_weights[i]));
        weight_sum += weights[i];
    }

    size_t i = 0;
    while (weight_sum > required_weight_sum)
    {
        if (weights[i] > 1 && static_cast<float>(weights[i]) > float_weights[i])
        {
            weights[i] -= 1;
            weight_sum -= 1;
        }
        i = (i + 1) % counts.size();
    }
    i = 0;
    while (weight_sum < required_weight_sum)
    {
        if (static_cast<float>(weights[i]) < float_weights[i])
        {
            weights[i] += 1;
            weight_sum += 1;
        }
        i = (i + 1) % counts.size();
    }
    return weights;
}

/// Choose both size_log and quantized weights (ans/encoding.rs::quantize_weights).
inline std::pair<Bitlen, PcoArray<Weight>> quantizeWeights(const PcoArray<Weight> & counts, size_t total_count, Bitlen max_size_log)
{
    if (counts.size() == 1)
        return {0, {1}};

    Bitlen min_size_log = static_cast<Bitlen>(32 - std::countl_zero(static_cast<uint32_t>(counts.size() - 1)));
    Bitlen size_log = std::max(min_size_log, max_size_log);
    PcoArray<Weight> weights = quantizeWeightsTo(counts, total_count, size_log);

    Bitlen power_of_2 = 32;
    for (Weight w : weights)
        power_of_2 = std::min(power_of_2, static_cast<Bitlen>(std::countr_zero(w)));
    size_log -= power_of_2;
    for (Weight & w : weights)
        w >>= power_of_2;
    return {size_log, weights};
}

}
