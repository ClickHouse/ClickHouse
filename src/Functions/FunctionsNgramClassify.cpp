#include <unordered_map>
#include <Functions/FunctionsStringSimilarity.h>
#include <Functions/FunctionsNgramClassify.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context_fwd.h>
#include <Functions/FunctionFactory.h>
#include <fstream>
#include <vector>
#include <memory>
#include "Interpreters/InterpreterCreateQuery.h"
#include "base/defines.h"
#include "base/types.h"

/*
- - - - - - - - - - - - - Чего я хочу - - - - - - - - - - - - - -
1. Разбивать строчки на ngram-мы | -> | кладем просто в хешмапу (хешируем)

Теперь для всех моделей, которые у нас есть: (через которые мы хотим посмотреть)
    2. Достаем хешмапу модели (нехешированную), хешируем ее
    3. Сливаем модельки -> в каждую хешмапу добавляем все уникальные ngram-ы другой
    4. Нормируем мапы 
    5. Считаем формулу Байеса (конечно же с логарифмами)
    6. Возвращаем вероятность принадлежности строки к модели

И дальше в зависимости от того, что нужно дальше 
    - Либо просто нормируем вероятности и возвращаем вероятность пренадлежности нашему классу
    - Либо сортируем всех (отнормированных) и возвращаем вектором пар
    - Либо просто возвращаем наиболее вероятную модельку
*/

namespace DB
{

struct NgramTextClassificationImpl
{
    using ResultType = String;
    static constexpr Float64 zero_frequency = 1e-06;
    
    template <typename ModelMap>
    static Float64 naiveBayes(
        const ModelMap & text, // text is normalized
        const ModelMap & model) // model is not normalized
    {
        int cnt_words_in_model = 0;

        for (const auto &[word, stat] : model)
        {
            cnt_words_in_model += stat;
        }

        for (const auto &[word, stat] : text)
        {
            if (model.find(word) == model.end())
            {
                ++cnt_words_in_model;
            }
        }


        Float64 result = 0;
        for (const auto &[word, stat] : text)
        {
            const auto it = model.find(word); 
            if (it == model.end())
            {
                result += stat * log(zero_frequency);
            } else
            {
                result += stat * log(stat / cnt_words_in_model);
            }
        }
        return result;
    }

    template<class ModelMap>
    class NgramModel {
        // модель, в которой лежит только одна запись какого-то slice-а
        // 
    public:
        explicit NgramModel(const String &filename) {
            load(filename);
        }
        /*
        Формат модели

        Имя N[сколько грам]
        [N-gram1] [cnt1]
        [N-gram2] [cnt2]
        ...
        */
        void load(const String &filename) { // загружаем из файла модель
            map.clear();
            std::ifstream in(filename);
            in >> name >> n;
            // String buffer(n + 1, '\0');
            char buffer[n + 1];
            for (size_t i = 0; i < n + 1; ++i) {
                buffer[i] = '\0';
            }
            int count = 0;
            while (!in.eof()) {
                in.read(buffer, n);
                in >> count;
                map[StringSlice(buffer, n)] = count;
            }
            in.close();
        }
        Float64 scoreText(const ModelMap &text_model) const {
            return naiveBayes(text_model, map);
        }
        void learn(const String &name_, size_t n_, const String &text) { // загружаем из строки модель
            name = name_;
            n = n_;
            map = buildModel<ModelMap>(text, n);
        }
        void learnDump(const String &name_, size_t n_, const String &text, const String &filename) { // will be used only for creating models
            name = name_;
            n = n_;
            std::unordered_map<String, int> slow_map;
            String tmp;
            tmp.clear();
            for (size_t index = 0; index < std::min(text.size(), n); ++index) {
                tmp += text[index];
            }
            ++slow_map[tmp];
            for (size_t index = 1; index + n < text.size(); ++index) {
                tmp.clear();
                for (size_t index2 = 0; index2 < n; ++index2) {
                    tmp += text[index + index2];
                }
                ++slow_map[tmp];
            }
            // here we need to print that model
            std::ofstream out(filename);
            out << name << ' ' << n << '\n';
            for (const auto &[word, count] : slow_map) {
                out << word << count << '\n';
            }
            out.close();
        }
        const ModelMap& getMap() const {
            return map;
        }
        String getName() const {
            return name;
        }
        size_t getN() const {
            return n;
        }
    private:
        size_t n;
        String name;
        ModelMap map;

        Float64 naiveBayes(
                const ModelMap & text, // text is normalized
                const ModelMap & model) const { // model is not normalized 
            int cnt_words_in_model = 0;

            for (const auto &[word, stat] : model) {
                cnt_words_in_model += stat;
            }

            for (const auto &[word, stat] : text) {
                if (model.find(word) == model.end()) {
                    ++cnt_words_in_model;
                }
            }

            Float64 result = 0;
            for (const auto &[word, stat] : text) {
                const auto it = model.find(word); 
                if (it == model.end()) {
                    result += stat * log(1 / cnt_words_in_model);
                } else {
                    result += stat * log(zero_frequency);
                }
            }
            return result;
        }


    };

    template<class ModelMap>
    class Slice {
        // for example, Lang-slice, explicity slice, ..., e.t.c.
    public:
        Slice() = default;
        explicit Slice(const std::string &directory) {
            load(directory);
        }

        String classify(const String &text) const {
            ModelMap text_map = buildModel<ModelMap>(text, n);
            Float64 best_score = 0.;
            String result;
            for (const auto &model : models) {
                Float64 local_result = model.scoreText(text_map);
                if (local_result > best_score) {
                    best_score = local_result;
                    result = model.getName();
                }
            }
            return result;
        }

        std::vector<std::pair<String, Float64>> score(const String &text, bool sorted = false) {
            ModelMap text_map = buildModel<ModelMap>(text, n);
            std::vector<std::pair<String, Float64>> result;
            for (const auto &model : models) {
                result.emplace_back(model.getName(), model.score(text_map));
            }
            if (sorted) {
                sort(result.begin(), result.end(), [](const auto &a, const auto &b){ return a.second > b.second; });
            }
            return result;
        }
    private:
        std::vector<NgramModel<ModelMap>> models;
        std::string name;
        size_t n;
        void load(const std::string &directory) {
            const std::filesystem::path path{directory};
            for (auto const& dir_entry : std::filesystem::directory_iterator{path}) {
                models.emplace_back(dir_entry.path());
            }
            if (models.empty()) {
                // BOOM
            }
            n = models[0].getN();
            for (const auto &model : models) {
                if (model.getN() != n) {
                    // BOOM
                }
            }
        }
    };

    struct StringSlice {
        const size_t p = 313;
        size_t precalced_pn;
        size_t length = 0;
        StringSlice() = default;
        size_t value = 0;
        StringSlice(const String &s, size_t n) {
            precalced_pn = 1;
            for (size_t index = 0; index < std::min(s.size(), n); ++index) {
                value *= p;
                value += static_cast<size_t>(s[index]);
                precalced_pn *= p;
            }
        }

        StringSlice(char *buf, size_t n) {
            precalced_pn = 1;
            for (size_t index = 0; index < std::min(strlen(buf), n); ++index) {
                value *= p;
                value += static_cast<size_t>(buf[index]);
                precalced_pn *= p;
            }
        }
        void slide(char old_c, char new_c) {
            value -= precalced_pn * static_cast<size_t>(old_c);
            value *= p;
            value += static_cast<size_t>(new_c);
        }
        bool operator == (const StringSlice& ss) const {
            return length == ss.length && value == ss.value;
        }
        size_t get() const {
            return value;
        }
    };

    class CustomHashFunction {
    public:
        size_t operator()(const StringSlice& p) const
        {
            return p.value + p.length;
        }
    };

    template <typename ModelMap>
   ModelMap static buildModel(const String &text, size_t n) {
        // depends on custom_Hash
        StringSlice ss(text, n);
        std::unordered_map<StringSlice, int, CustomHashFunction> map;
        ++map[ss];
        for (size_t index = n; index < text.size(); ++index) {
            ss.slide(text[index - n], text[index]);
            ++map[ss];
        }
        return map;
    }

    template <typename ModelMap>
    Float64 ngramScore(const String &text, const NgramModel<ModelMap> &model) {
        // depends on buildModel
        ModelMap text_model = buildModel<ModelMap>(text, model.getN());
        return naiveBayes(text_model, model.getMap());
    }

    // path will be "~/ClickHouse/src/Storages/NgramModels/[slice-Name]-[N]/"
    static void constant(std::string data, const String &slice_name, String &res) {
        Slice<std::unordered_map<StringSlice, int, CustomHashFunction> > slice("~/ClickHouse/src/Storages/NgramModels/" + slice_name);
        res = slice.classify(data);
    }

    static void vector(const ColumnString::Chars & data, const ColumnString::Offsets &offsets, const String &slice_name, ColumnString::Chars &res_data, ColumnString::Offsets &res_offsets) {
        Slice<std::unordered_map<StringSlice, int, CustomHashFunction> > slice("~/ClickHouse/src/Storages/NgramModels/" + slice_name);
        size_t prev_offset = 0;
        for (size_t i = 0; i < offsets.size(); ++i) {
            const UInt8 * haystack = &data[prev_offset];
            const String &result_value = slice.classify(reinterpret_cast<const char *>(haystack));
            res_data.resize(prev_offset + result_value.size() + 1);
            prev_offset = offsets[i];
            memcpy(&res_data[prev_offset], result_value.data(), result_value.size());
            res_data[prev_offset + result_value.size()] = '\0';
            prev_offset += result_value.size() + 1;
            res_offsets[i] = prev_offset;
        }
    }
    
};

struct NgramClassificationName
{
    static constexpr auto name = "NgramClassificationProb";
};

using FunctionNgramTextClassification = NgramTextClassification<NgramTextClassificationImpl, NgramClassificationName>;

REGISTER_FUNCTION(NgramClassify)
{
    factory.registerFunction<FunctionNgramTextClassification>();
}

}
