// #include <algorithm>
// #include <cmath>
// #include <string>
// #include <string_view>
// #include <vector>
// #include <absl/container/flat_hash_set.h>
// #include <Common/HashTable/HashMap.h>
// #include <Common/HashTable/HashSet.h>

// namespace DB
// {

// using Rune = UInt32;
// struct RuneInfo
// {
//     Rune rune = 0;
//     UInt32 offset = 0;
//     UInt32 len = 0;
// };

// struct RuneRange
// {
//     UInt32 begin = 0;
//     UInt32 end = 0;
// };
// using Word = std::string_view;

// inline RuneInfo decodeUTF8Rune(const char * str, size_t len)
// {
//     RuneInfo info;
//     if (!str || len == 0)
//         return info;

//     uint8_t first = static_cast<uint8_t>(str[0]);

//     if (!(first & 0x80))
//     {
//         info.rune = first & 0x7f;
//         info.len = 1;
//     }
//     else if (first <= 0xdf && len >= 2)
//     {
//         info.rune = (first & 0x1f) << 6 | (static_cast<uint8_t>(str[1]) & 0x3f);
//         info.len = 2;
//     }
//     else if (first <= 0xef && len >= 3)
//     {
//         info.rune = (first & 0x0f) << 12 | (static_cast<uint8_t>(str[1]) & 0x3f) << 6 | (static_cast<uint8_t>(str[2]) & 0x3f);
//         info.len = 3;
//     }
//     else if (first <= 0xf7 && len >= 4)
//     {
//         info.rune = (first & 0x07) << 18 | (static_cast<uint8_t>(str[1]) & 0x3f) << 12 | (static_cast<uint8_t>(str[2]) & 0x3f) << 6
//             | (static_cast<uint8_t>(str[3]) & 0x3f);
//         info.len = 4;
//     }

//     return info;
// }

// inline bool decodeUTF8String(std::string_view str, std::vector<RuneInfo> & runes)
// {
//     runes.clear();
//     runes.reserve(str.size() / 2);

//     size_t pos = 0;
//     while (pos < str.size())
//     {
//         RuneInfo info = decodeUTF8Rune(str.data() + pos, str.size() - pos);
//         if (info.len == 0)
//             return false;

//         info.offset = pos;
//         runes.push_back(info);

//         pos += info.len;
//     }

//     return true;
// }

// struct DictUnit
// {
//     std::vector<Rune> word;
//     double weight = 0.0;
// };

// class TrieNode
// {
// public:
//     HashMap<Rune, TrieNode *> children;
//     const DictUnit * value = nullptr;

//     ~TrieNode()
//     {
//         for (auto & [_, child] : children)
//             delete child;
//     }
// };


// class DictTrie
// {
//     static constexpr size_t MAX_WORD_LENGTH = 512;

// public:
//     struct DagNode
//     {
//         std::vector<std::pair<size_t, const DictUnit *>> nexts;
//         const DictUnit * best_match = nullptr;
//         double weight = -1e100;
//     };

//     explicit DictTrie(std::string_view dict_data)
//     {
//         loadDict(dict_data);
//         calculateWeights();
//     }

//     ~DictTrie() { delete root; }

//     const DictUnit * find(const std::vector<RuneInfo> & runes, size_t start, size_t end) const
//     {
//         if (start >= end || runes.empty())
//             return nullptr;

//         TrieNode * node = root;
//         for (size_t i = start; i < end && i < runes.size(); i++)
//         {
//             auto it = node->children.find(runes[i].rune);
//             if (it == node->children.end())
//                 return nullptr;
//             node = it->getMapped();
//         }

//         return node->value;
//     }

//     std::vector<DagNode> buildDAG(const std::vector<RuneInfo> & runes, size_t begin, size_t end) const
//     {
//         std::vector<DagNode> dag;
//         dag.resize(end - begin);
//         for (size_t i = begin; i < end; i++)
//         {
//             TrieNode * node = root;
//             for (size_t j = i; j < end && j - i < MAX_WORD_LENGTH; ++j)
//             {
//                 auto it = node->children.find(runes[j].rune);
//                 if (it == node->children.end())
//                     break;

//                 node = it->getMapped();
//                 if (node->value)
//                     dag[i].nexts.emplace_back(j, node->value);
//             }

//             if (dag[i].nexts.empty())
//                 dag[i].nexts.emplace_back(i, nullptr);
//         }

//         return dag;
//     }

//     double getMinWeight() const { return min_weight; }

// private:
//     void loadDict(std::string_view data)
//     {
//         size_t pos = 0;

//         while (pos < data.size())
//         {
//             size_t line_end = data.find('\n', pos);
//             if (line_end == std::string_view::npos)
//                 line_end = data.size();

//             std::string_view line = data.substr(pos, line_end - pos);
//             pos = line_end + 1;

//             if (line.empty())
//                 continue;

//             size_t pos1 = line.find(' ');
//             if (pos1 == std::string_view::npos)
//                 continue;

//             size_t pos2 = line.find(' ', pos1 + 1);
//             if (pos2 == std::string_view::npos)
//                 continue;

//             std::string_view word_str = line.substr(0, pos1);
//             std::string_view freq_str = line.substr(pos1 + 1, pos2 - pos1 - 1);

//             DictUnit unit;
//             std::vector<RuneInfo> runes;
//             if (!decodeUTF8String(word_str, runes))
//                 continue;

//             unit.word.reserve(runes.size());
//             for (const auto & r : runes)
//                 unit.word.push_back(r.rune);

//             char * end_ptr = nullptr;
//             unit.weight = std::strtod(std::string(freq_str).c_str(), &end_ptr);

//             insertWord(unit);
//         }
//     }

//     void insertWord(const DictUnit & unit)
//     {
//         dict_units.push_back(unit);
//         const DictUnit * ptr = &dict_units.back();

//         TrieNode * node = root;
//         for (Rune rune : unit.word)
//         {
//             auto it = node->children.find(rune);
//             if (it == node->children.end())
//             {
//                 TrieNode * new_node = new TrieNode();
//                 node->children.insert({rune, new_node});
//                 node = new_node;
//             }
//             else
//             {
//                 node = it->getMapped();
//             }
//         }
//         node->value = ptr;
//     }

//     void calculateWeights()
//     {
//         double sum = 0;
//         for (auto & unit : dict_units)
//             sum += unit.weight;

//         if (sum > 0)
//         {
//             for (auto & unit : dict_units)
//                 unit.weight = std::log(unit.weight / sum);
//         }

//         min_weight = 0;
//         for (const auto & unit : dict_units)
//             min_weight = std::min(min_weight, unit.weight);
//     }

//     TrieNode * root = new TrieNode();
//     std::vector<DictUnit> dict_units;
//     double min_weight = -1e100;
// };

// class HMMModel
// {
// public:
//     explicit HMMModel(std::string_view model_data) { loadModel(model_data); }

//     void viterbi(const std::vector<RuneInfo> & runes, std::vector<State> & states) const
//     {
//         states.clear();

//         size_t len = runes.size();
//         if (len == 0)
//             return;

//         states.resize(len, B);
//         if (len == 1)
//         {
//             states[0] = S;
//             return;
//         }

//         std::vector<std::vector<double>> weight(len, std::vector<double>(STATE_COUNT, -1e100));
//         std::vector<std::vector<State>> path(len, std::vector<State>(STATE_COUNT, B));

//         for (size_t s = 0; s < STATE_COUNT; s++)
//             weight[0][s] = start_prob[s] + getEmitProb(s, runes[0].rune);

//         for (size_t i = 1; i < len; i++)
//         {
//             for (size_t curr = 0; curr < STATE_COUNT; curr++)
//             {
//                 double emit = getEmitProb(curr, runes[i].rune);

//                 for (size_t prev = 0; prev < STATE_COUNT; prev++)
//                 {
//                     double score = weight[i - 1][prev] + trans_prob[prev][curr] + emit;
//                     if (score > weight[i][curr])
//                     {
//                         weight[i][curr] = score;
//                         path[i][curr] = static_cast<State>(prev);
//                     }
//                 }
//             }
//         }

//         State best_state = weight[len - 1][E] > weight[len - 1][S] ? E : S;
//         for (int i = static_cast<int>(len) - 1; i >= 0; i--)
//         {
//             states[i] = best_state;
//             if (i > 0)
//                 best_state = path[i][best_state];
//         }
//     }

// private:
//     void loadModel(std::string_view data)
//     {
//         size_t pos = 0;

//         auto read_line = [&]() -> std::string_view
//         {
//             if (pos >= data.size())
//                 return {};

//             size_t line_end = data.find('\n', pos);
//             if (line_end == std::string_view::npos)
//                 line_end = data.size();

//             std::string_view line = data.substr(pos, line_end - pos);
//             pos = line_end + 1;
//             return line;
//         };

//         auto parse_probs = [](std::string_view line, double * out, size_t count)
//         {
//             size_t idx = 0;
//             size_t p = 0;

//             while (p < line.size() && idx < count)
//             {
//                 while (p < line.size() && line[p] == ' ')
//                     p++;

//                 if (p >= line.size())
//                     break;

//                 size_t next = p;
//                 while (next < line.size() && line[next] != ' ')
//                     next++;

//                 std::string_view token = line.substr(p, next - p);
//                 if (!token.empty())
//                 {
//                     char * end_ptr = nullptr;
//                     out[idx++] = std::strtod(std::string(token).c_str(), &end_ptr);
//                 }
//                 p = next;
//             }
//         };

//         auto parse_emit_prob = [](std::string_view line, HashMap<Rune, double> & mp)
//         {
//             size_t p = 0;

//             while (p < line.size())
//             {
//                 // 找到下一个逗号
//                 size_t comma = line.find(',', p);
//                 if (comma == std::string_view::npos)
//                     comma = line.size();

//                 std::string_view token = line.substr(p, comma - p);

//                 // 找到冒号
//                 size_t colon = token.find(':');
//                 if (colon != std::string_view::npos)
//                 {
//                     std::string_view word_str = token.substr(0, colon);
//                     std::string_view prob_str = token.substr(colon + 1);

//                     std::vector<RuneInfo> runes;
//                     if (decodeUTF8String(word_str, runes) && !runes.empty())
//                     {
//                         char * end_ptr = nullptr;
//                         double prob = std::strtod(std::string(prob_str).c_str(), &end_ptr);
//                         mp.insert({runes[0].rune, prob});
//                     }
//                 }

//                 p = comma + 1;
//             }
//         };

//         // 读取发射概率
//         parse_emit_prob(read_line(), emit_prob_b);
//         parse_emit_prob(read_line(), emit_prob_e);
//         parse_emit_prob(read_line(), emit_prob_m);
//         parse_emit_prob(read_line(), emit_prob_s);
//     }

//     double getEmitProb(size_t state, Rune rune) const
//     {
//         const HashMap<Rune, double> * map = nullptr;

//         switch (state)
//         {
//             case B:
//                 map = &emit_prob_b;
//                 break;
//             case E:
//                 map = &emit_prob_e;
//                 break;
//             case M:
//                 map = &emit_prob_m;
//                 break;
//             case S:
//                 map = &emit_prob_s;
//                 break;
//             default:
//                 return -1e100;
//         }

//         auto it = map->find(rune);
//         if (it == map->end())
//             return -8.0;
//         return it->getMapped();
//     }

//     enum State
//     {
//         B = 0,
//         E = 1,
//         M = 2,
//         S = 3
//     };

//     constexpr int STATE_COUNT = 4;

//     std::array<double, STATE_COUNT> start_prob = {
//         -0.26268660809250016, // B
//         -3.14e+100, // E
//         -3.14e+100, // M
//         -1.4652633398537678 // S
//     };

//     std::array<std::array<double, STATE_COUNT>, STATE_COUNT> trans_prob = {{
//         {-3.14e+100, -0.510825623765990, -0.916290731874155, -3.14e+100}, // B -> *
//         {-0.5897149736854513, -3.14e+100, -3.14e+100, -0.8085250474669937}, // E -> *
//         {-3.14e+100, -0.33344856811948514, -1.2603623820268226, -3.14e+100}, // M -> *
//         {-0.7211965654669841, -3.14e+100, -3.14e+100, -0.6658631448798212} // S -> *
//     }};

//     HashMap<Rune, double> emit_prob_b;
//     HashMap<Rune, double> emit_prob_e;
//     HashMap<Rune, double> emit_prob_m;
//     HashMap<Rune, double> emit_prob_s;
// };

// class MPSegment
// {
// public:
//     explicit MPSegment(const DictTrie & dict_trie_)
//         : dict_trie(dict_trie_)
//     {
//     }

//     void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges) const
//     {
//         if (runes.empty())
//             return;

//         std::vector<DictTrie::DagNode> dag = dict_trie.buildDAG(runes, begin, end);
//         for (int i = static_cast<int>(dag.size()) - 1; i >= 0; --i)
//         {
//             for (const auto & [next_pos, unit] : dag[i].nexts)
//             {
//                 double weight = unit ? unit->weight : dict_trie.getMinWeight();
//                 if (next_pos + 1 < dag.size())
//                     weight += dag[next_pos + 1].weight;

//                 if (weight > dag[i].weight)
//                 {
//                     dag[i].weight = weight;
//                     dag[i].best_match = unit;
//                 }
//             }
//         }

//         size_t pos = 0;
//         while (pos < dag.size())
//         {
//             const DictUnit * unit = dag[pos].best_match;
//             size_t word_len = unit ? unit->word.size() : 1;

//             ranges.emplace_back(pos, pos + word_len);
//             pos += word_len;
//         }
//     }

// private:
//     const DictTrie & dict_trie;
// };

// class HMMSegment
// {
// public:
//     explicit HMMSegment(const HMMModel & model_)
//         : model(model_)
//     {
//     }

//     void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges) const
//     {
//         if (begin >= end || runes.empty())
//             return;

//         std::vector<RuneInfo> segment_runes(runes.begin() + begin, runes.begin() + end);

//         std::vector<HMMModel::State> states;
//         model.viterbi(segment_runes, states);

//         size_t word_start = 0;
//         for (size_t i = 0; i < states.size(); i++)
//         {
//             if (states[i] == HMMModel::E || states[i] == HMMModel::S)
//             {
//                 ranges.emplace_back(begin + word_start, begin + i + 1);
//                 word_start = i + 1;
//             }
//         }

//         if (word_start < states.size())
//             ranges.emplace_back(begin + word_start, begin + states.size());
//     }

// private:
//     const HMMModel & model;
// };

// class MixSegment
// {
// public:
//     MixSegment(const DictTrie & dict_trie, const HMMModel & model)
//         : mp_seg(dict_trie)
//         , hmm_seg(model)
//     {
//     }

//     void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges, bool use_hmm = true) const
//     {
//         if (!use_hmm)
//         {
//             mp_seg.cut(runes, begin, end, ranges);
//             return;
//         }

//         std::vector<RuneRange> mp_ranges;
//         mp_seg.cut(runes, begin, end, mp_ranges);
//         for (size_t i = 0; i < mp_ranges.size(); ++i)
//         {
//             size_t start = mp_ranges[i].first;
//             size_t end_pos = mp_ranges[i].second;

//             if (end_pos - start > 1)
//             {
//                 ranges.emplace_back(start, end_pos);
//             }
//             else
//             {
//                 size_t single_start = start;
//                 size_t single_end = end_pos;

//                 for (size_t j = i + 1; j < mp_ranges.size(); ++j)
//                 {
//                     size_t next_start = mp_ranges[j].first;
//                     size_t next_end = mp_ranges[j].second;

//                     if (next_start != single_end)
//                         break;

//                     if (next_end - next_start == 1)
//                         single_end = next_end;
//                     else
//                         break;
//                 }

//                 if (single_end > single_start)
//                     hmm_seg.cut(runes, single_start, single_end, ranges);
//             }
//         }
//     }

// private:
//     MPSegment mp_seg;
//     HMMSegment hmm_seg;
// };

// class QuerySegment
// {
// public:
//     QuerySegment(const DictTrie & dict_trie_, const HMMModel & model)
//         : mix_seg(dict_trie_, model)
//         , dict_trie(dict_trie_)
//     {
//     }

//     void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges, bool use_hmm = true) const
//     {
//         std::vector<RuneRange> mix_ranges;
//         mix_seg.cut(runes, begin, end, mix_ranges, use_hmm);

//         for (const auto & [start, end_pos] : mix_ranges)
//         {
//             size_t len = end_pos - start;

//             if (len > 2)
//             {
//                 for (size_t i = start; i + 1 < end_pos; i++)
//                 {
//                     const DictUnit * unit = dict_trie.find(runes, i, i + 2);
//                     if (unit)
//                         ranges.emplace_back(i, i + 2);
//                 }
//             }

//             if (len > 3)
//             {
//                 for (size_t i = start; i + 2 < end_pos; i++)
//                 {
//                     const DictUnit * unit = dict_trie.find(runes, i, i + 3);
//                     if (unit)
//                         ranges.emplace_back(i, i + 3);
//                 }
//             }

//             ranges.emplace_back(start, end_pos);
//         }
//     }

// private:
//     MixSegment mix_seg;
//     const DictTrie & dict_trie;
// };

// class FullSegment
// {
// public:
//     explicit FullSegment(const DictTrie & dict_trie_)
//         : dict_trie(dict_trie_)
//     {
//     }

//     void cut(const std::vector<RuneInfo> & runes, size_t begin, size_t end, std::vector<RuneRange> & ranges) const
//     {
//         std::vector<DictTrie::DagNode> dag = dict_trie.buildDAG(runes, begin, end);

//         absl::flat_hash_set<RuneRange> unique_ranges;
//         for (size_t i = 0; i < dag.size(); i++)
//         {
//             for (const auto & [next_pos, unit] : dag[i].nexts)
//             {
//                 size_t word_len = next_pos - i + 1;
//                 if (unit || (word_len == 1 && dag[i].nexts.size() == 1))
//                     unique_ranges.emplace(begin + i, begin + next_pos + 1);
//             }
//         }

//         for (const auto & range : unique_ranges)
//             ranges.push_back(range);
//     }

// private:
//     const DictTrie & dict_trie;
// };

// class PreFilter
// {
// public:
//     PreFilter(const absl::flat_hash_set<Rune> & separators_, const std::vector<RuneInfo> & runes_)
//         : separators(separators_)
//         , runes(runes_)
//     {
//     }

//     bool hasNext() const { return cursor < runes.size(); }

//     RuneRange next()
//     {
//         RuneRange range;
//         range.begin = cursor;

//         while (cursor < runes.size())
//         {
//             if (separators.has(runes[cursor].rune))
//             {
//                 if (range.begin == cursor)
//                     ++cursor;
//                 range.end = cursor;
//                 return range;
//             }
//             ++cursor;
//         }

//         range.end = runes.size();
//         return range;
//     }

// private:
//     const absl::flat_hash_set<Rune> & separators;
//     const std::vector<RuneInfo> & runes;
//     size_t cursor = 0;
// };

// class Jieba
// {
// public:
//     Jieba(std::string_view dict_data, std::string_view model_data)
//         : dict_trie(dict_data)
//         , hmm_model(model_data)
//         , mp_seg(dict_trie)
//         , hmm_seg(hmm_model)
//         , mix_seg(dict_trie, hmm_model)
//         , query_seg(dict_trie, hmm_model)
//         , full_seg(dict_trie)
//     {
//         separators.insert(' ');
//         separators.insert('\t');
//         separators.insert('\n');

//         separators.insert(0xFF0C); // ，
//         separators.insert(0x3002); // 。
//     }

//     // 混合分词（MP + HMM）
//     void cut(std::string_view sentence, std::vector<Word> & words, bool use_hmm = true)
//     {
//         words.clear();

//         std::vector<RuneInfo> runes;
//         if (!decodeUTF8String(sentence, runes))
//             return;

//         if (runes.empty())
//             return;

//         PreFilter filter(separators_, runes);
//         std::vector<RuneRange> all_ranges;

//         while (filter.hasNext())
//         {
//             PreFilter::Range range = filter.next();
//             if (range.begin >= range.end)
//                 continue;

//             mix_seg.cut(runes, range.begin, range.end, all_ranges, use_hmm);
//         }

//         // 转换为Word
//         convertRangesToWords(sentence, runes, all_ranges, words);
//     }

//     // 完全分词（枚举所有可能的词）
//     void cutAll(std::string_view sentence, std::vector<Word> & words)
//     {
//         words.clear();

//         std::vector<RuneInfo> runes;
//         if (!decodeUTF8String(sentence, runes))
//             return;

//         if (runes.empty())
//             return;

//         PreFilter filter(separators_, runes);
//         std::vector<RuneRange> all_ranges;

//         while (filter.hasNext())
//         {
//             PreFilter::Range range = filter.next();
//             if (range.begin >= range.end)
//                 continue;

//             full_seg_.cut(runes, range.begin, range.end, all_ranges);
//         }

//         convertRangesToWords(sentence, runes, all_ranges, words);
//     }

//     void cutForSearch(std::string_view sentence, std::vector<Word> & words, bool use_hmm = true)
//     {
//         words.clear();

//         std::vector<RuneInfo> runes;
//         if (!decodeUTF8String(sentence, runes))
//             return;

//         if (runes.empty())
//             return;

//         std::vector<RuneRange> all_ranges;

//         while (filter.hasNext())
//         {
//             PreFilter::Range range = filter.next();
//             if (range.begin >= range.end)
//                 continue;

//             query_seg_.cut(runes, range.begin, range.end, all_ranges, use_hmm);
//         }

//         convertRangesToWords(sentence, runes, all_ranges, words);
//     }

//     void cutHMM(std::string_view sentence, std::vector<Word> & words)
//     {
//         words.clear();

//         std::vector<RuneInfo> runes;
//         if (!decodeUTF8String(sentence, runes))
//             return;

//         if (runes.empty())
//             return;

//         std::vector<RuneRange> all_ranges;

//         while (filter.hasNext())
//         {
//             PreFilter::Range range = filter.next();
//             if (range.begin >= range.end)
//                 continue;

//             hmm_seg_.cut(runes, range.begin, range.end, all_ranges);
//         }

//         convertRangesToWords(sentence, runes, all_ranges, words);
//     }

//     void cutSmall(std::string_view sentence, std::vector<Word> & words)
//     {
//         words.clear();

//         std::vector<RuneInfo> runes;
//         if (!decodeUTF8String(sentence, runes))
//             return;

//         if (runes.empty())
//             return;

//         std::vector<RuneRange> all_ranges;

//         while (filter.hasNext())
//         {
//             PreFilter::Range range = filter.next();
//             if (range.begin >= range.end)
//                 continue;

//             mp_seg_.cut(runes, range.begin, range.end, all_ranges);
//         }

//         convertRangesToWords(sentence, runes, all_ranges, words);
//     }

// private:
//     void convertRangesToWords(
//         std::string_view sentence,
//         const std::vector<RuneInfo> & runes,
//         const std::vector<RuneRange> & ranges,
//         std::vector<Word> & words) const
//     {
//         words.reserve(ranges.size());
//         for (const auto & [start, end] : ranges)
//         {
//             size_t byte_start = runes[start].offset;
//             size_t byte_end = runes[end].offset + runes[end].len;
//             Word word = sentence.substr(byte_start, byte_end - byte_start);
//             words.push_back(word);
//         }
//     }

//     DictTrie dict_trie;
//     HMMModel hmm_model;
//     MPSegment mp_seg;
//     HMMSegment hmm_seg;
//     MixSegment mix_seg;
//     QuerySegment query_seg;
//     FullSegment full_seg;
//     absl::flat_hash_set<Rune> separators;
// };

// } // namespace DB
