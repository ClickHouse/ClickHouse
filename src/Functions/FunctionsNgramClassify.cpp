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
#include "Interpreters/InterpreterCreateQuery.h"
#include "base/defines.h"

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
    using ResultType = Float64;
    template <typename ModelMap>
    ALWAYS_INLINE inline Float64 naiveBayes(
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
                result += stat * log(1 / cnt_words_in_model);
            } else
            {
                result += stat * log(it->stat / cnt_words_in_model);
            }
        }
        return result;
    }

    template<class ModelMap>
    class NgramModel {
    public:
        explicit NgramModel(const String &filename) {
            load(filename);
        }
        void load(const String &filename) {
            map.clear();
            std::ifstream in(filename);
            in >> name >> n;
            char buffer[n + 1] = {0};
            int count = 0;
            while (!in.eof()) {
                in.read(&buffer, n);
                in >> count;
                map[StringSlice(buffer, n)] = count;
            }
            in.close();
        }
        void learn(const String &name_, size_t n_, const String &text) {
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
        int getN() const {
            return n;
        }
    private:
        size_t n;
        String name;
        ModelMap map;
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

    template <typename ModelMap>
    ALWAYS_INLINE std::unordered_map<size_t, int> static buildModel(const String &text, size_t n) {
        // depends on custom_Hash
        StringSlice ss(text, n);
        std::unordered_map<size_t, int> map;
        ++map[ss.get()];
        for (size_t index = n; index < text.size(); ++index) {
            ss.slide(text[index - n], text[index]);
            ++map[ss.get()];
        }
        return map;
    }

    template <typename ModelMap>
    ALWAYS_INLINE Float64 ngramScore(const String &text, const NgramModel<ModelMap> &model) {
        // depends on buildModel
        ModelMap text_model = buildModel<ModelMap>(text, model.getN());
        return naiveBayes(text_model, model.getMap());
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
