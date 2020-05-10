#pragma once

#include <vector>
#include <list>
#include <map>
#include <memory>
#include <ctime>

#include <boost/noncopyable.hpp>

#include <re2/re2.h>
#include <sparsehash/dense_hash_map>

#include <../base/common/StringRef.h>
#include <dbms/src/IO/ReadBuffer.h>
#include <dbms/src/IO/ReadHelpers.h>

#include <contrib/libs/clickhouse/libs/libcommon/include/common/logger_useful.h>

#include <library/cpp/on_disk/aho_corasick/reader.h>
#include <library/cpp/on_disk/aho_corasick/writer.h>

/** Более быстрый вариант реализации библиотеки uatraits.
  * Использует те же исходные данные, что и библиотека uatraits.
  * Всё остальное сделано по-другому.
  */

namespace Poco { namespace XML { class Node; } }

template <typename type>
class RootRule;

class UATraits : private boost::noncopyable
{
public:
    struct Version
    {
        unsigned v1 = 0;
        unsigned v2 = 0;
        unsigned v3 = 0;
        unsigned v4 = 0;

        bool empty() const;
        std::string toString() const;
        bool operator==(const Version & rhs) const
        {
            return std::tie(v1, v2, v3, v4) == std::tie(rhs.v1, rhs.v2, rhs.v3, rhs.v4);
        }
        bool operator!=(const Version & rhs) const
        {
            return !operator==(rhs);
        }
        bool operator>(const Version & rhs) const
        {
            return std::tie(v1, v2, v3, v4) > std::tie(rhs.v1, rhs.v2, rhs.v3, rhs.v4);
        }
        bool operator<(const Version & rhs) const
        {
            return std::tie(v1, v2, v3, v4) < std::tie(rhs.v1, rhs.v2, rhs.v3, rhs.v4);
        }
        bool operator>=(const Version & rhs) const
        {
            return operator==(rhs) || operator>(rhs);
        }
        bool operator<=(const Version & rhs) const
        {
            return operator==(rhs) || operator<(rhs);
        }
    };


    struct Result
    {
        /// При расширении, модифицируйте два места:

        enum StringRefFields                                    /// 1
        {
            OSFamily = 0,
            OSName = 1,
            BrowserEngine = 2,
            BrowserName = 3,
            BrowserShell = 4,
            BrowserBase = 5,
            Vendor = 6,
            DeviceVendor = 7,
            DeviceModel = 8,
            DeviceKeyboard = 9,
            DeviceName = 10,
            StringRefFieldsCount
        };

        StringRef string_ref_fields[StringRefFieldsCount];

        static const char ** stringRefFieldNames()
        {
            static const char * ret[] =                            /// 2
            {
                "OSFamily",
                "OSName",
                "BrowserEngine",
                "BrowserName",
                "BrowserShell",
                "BrowserBase",
                "Vendor",
                "DeviceVendor",
                "DeviceModel",
                "DeviceKeyboard",
                "DeviceName",
                nullptr,
            };

            return ret;
        }


        enum VersionFields
        {
            OSVersion = 0,
            BrowserVersion = 1,
            BrowserEngineVersion = 2,
            BrowserShellVersion = 3,
            BrowserBaseVersion = 4,
            GoogleToolBarVersion = 5,
            YandexBarVersion = 6,
            MailRuAgentVersion = 7,
            MailRuSputnikVersion = 8,
            YaITPExpVersion = 9,
            VersionFieldsCount
        };

        Version version_fields[VersionFieldsCount];

        static const char ** versionFieldNames()
        {
            static const char * ret[] =
            {
                "OSVersion",
                "BrowserVersion",
                "BrowserEngineVersion",
                "BrowserShellVersion",
                "BrowserBaseVersion",
                "GoogleToolBarVersion",
                "YandexBarVersion",
                "MailRuAgentVersion",
                "MailRuSputnikVersion",
                "YaITPExpVersion",
                nullptr,
            };

            return ret;
        }


        enum BoolFields
        {
            isMobile        = 0,
            isTablet        = 1,
            isRobot         = 2,
            PreferMobile    = 3,
            J2ME            = 4,
            isTouch            = 5,
            MultiTouch        = 6,
            isWAP            = 7,
            x64                = 8,
            isBrowser        = 9,
            isEmulator        = 10,
            GoogleToolBar    = 11,
            YandexBar        = 12,
            MailRuAgent        = 13,
            MailRuSputnik    = 14,
            isBeta            = 15,
            inAppBrowser    = 16,
            historySupport    = 17,
            isTV            = 18,
            postMessageSupport = 19,
            localStorageSupport = 20,
            SVGSupport = 21,
            WebPSupport = 22,
            CSP2Support = 23,
            CSP1Support = 24,
            ITP = 25,
            ITPMaybe = 26,
            ITPFakeCookie = 27,
            SameSiteSupport = 28,
            BoolFieldsCount
        };

        bool bool_fields[BoolFieldsCount] = {};

        static const char ** boolFieldNames()
        {
            static const char * ret[] =
            {
                "isMobile",
                "isTablet",
                "isRobot",
                "PreferMobile",
                "J2ME",
                "isTouch",
                "MultiTouch",
                "isWAP",
                "x64",
                "isBrowser",
                "isEmulator",
                "GoogleToolBar",
                "YandexBar",
                "MailRuAgent",
                "MailRuSputnik",
                "isBeta",
                "inAppBrowser",
                "historySupport",
                "isTV",
                "postMessageSupport",
                "localStorageSupport",
                "SVGSupport",
                "WebPSupport",
                "CSP2Support",
                "CSP1Support",
                "ITP",
                "ITPMaybe",
                "ITPFakeCookie",
                "SameSiteSupport",
                nullptr,
            };

            return ret;
        }


        enum UIntFields
        {
            ScreenWidth        = 0,
            ScreenHeight    = 1,
            BitsPerPixel    = 2,
            UIntFieldsCount
        };

        unsigned uint_fields[UIntFieldsCount] = {};

        static const char ** uIntFieldNames()
        {
            static const char * ret[] =
            {
                "ScreenWidth",
                "ScreenHeight",
                "BitsPerPixel",
                nullptr,
            };

            return ret;
        }


        enum IgnoredFields
        {
            ScreenSize = 0,
            IgnoredFieldsCount
        };

        static const char ** ignoredFieldNames()
        {
            static const char * ret[] =
            {
                "ScreenSize",
                nullptr,
            };

            return ret;
        }


        /// Хранит UserAgent в случае, если на него есть ссылки.
        std::string user_agent_cache;

        /// Хранит X-OperaMini-Phone-UA в случае, если на него есть ссылки.
        std::string x_operamini_phone_ua_cache;


        Result();
        const Result & operator =(const Result & right);

        /// Отладочный вывод. Выводит только ненулевые значения.
        void dump(std::ostream & ostr) const;
    };

    enum SourceType
    {
        SourceUserAgent,
        SourceXOperaMiniPhoneUA
    };


    /** Принимает путь к файлам browser.xml, profiles.xml и extra.xml.
      * Инициализация может занять существенное время.
      */
    UATraits(const std::string & browser_path, const std::string & profiles_path, const std::string & extra_path);
    /** Принимает содержимое файлов browser.xml, profiles.xml и extra.xml.
      * Инициализация может занять существенное время.
      */
    UATraits(std::istream & browser_istr, std::istream & profiles_istr, std::istream & extra_path_istr);
    ~UATraits();
    /** Перезагрузить данные, если хотя бы один файл был модифицирован после предыдущей загрузки.
      * Если перезагрузка произошла(метод вернет true), все полученные ранее объекты Result инвалидируются
      * (так как ссылаются на строчки из старого файла).
      */
    bool load();


    /** Структура данных, модифицируемая во время работы.
      */
    struct Match
    {
        static const UInt8 max_positions = 3;
        UInt8 positions_len = 0;
        UInt16 positions[max_positions];
    };

    static const size_t MAX_USER_AGENT_LENGTH = 0xFFFF;    /// Нужно для UInt16 position выше.

    using MatchedSubstrings = std::vector<Match>;

    /// Определить всё, что можно.
    ///
    /// В качестве user_agent_lower передайте значение заголовка User-Agent, заранее переведённое в нижний регистр.
    /// В качестве profile передайте значение заголовка X-WAP-Profile или Profile.
    /// В качестве x_operamini_phone_ua_lower передайте значение соответствующего заголовка, заранее переведённое в нижний регистр.
    ///
    /// В качестве result передайте пустой (новый) объект Result.
    ///
    /// В качестве matched_substrings передайте заранее созданный объект MatchedSubstrings
    ///  (в нём будет расположен кусок памяти для работы), чтобы не пришлось создавать его каждый раз заново.
    ///  Функция сама очищает и ресайзит его (делать это самому не нужно).
    ///
    /// Функцию можно вызывать одновременно из разных потоков.
    ///
    /// user_agent_lower и x_operamini_phone_ua_lower могут использоваться в выходных StringRef.
    void detect(
        StringRef user_agent_lower, StringRef profile, StringRef x_operamini_phone_ua_lower,
        Result & result,
        MatchedSubstrings & matched_substrings) const;

    /// Определить всё, что можно и восстановить регистр в результирующих полях.
    /// См. комментарий к detect(). Кроме того:
    /// user_agent_lower должна соответствовать user_agent по байтовым смещениям символов
    /// x_operamini_phone_ua_lower должна соответствовать x_operamini_phone_ua
    ///
    /// user_agent и x_operamini_phone_ua могут использоваться в выходных StringRef.
    void detectCaseSafe(
        StringRef user_agent, StringRef user_agent_lower, StringRef profile,
        StringRef x_operamini_phone_ua, StringRef x_operamini_phone_ua_lower,
        Result & result,
        MatchedSubstrings & matched_substrings) const;


    /** По DeviceModel найти DeviceName. Если не найдено - вернуть пустую строку.
      */
    std::string getNameByModel(const std::string & model) const;

    /// Обработчик значения для поля
    struct IAction
    {
        /// Что делать, когда определённый узел дерева достигнут, и все необходимые условия сработали.
        virtual void execute(StringRef user_agent, Result & result, size_t subpatterns_size, re2::StringPiece * subpatterns) const = 0;
        virtual ~IAction() {}
    };

private:
    using Regexp = std::unique_ptr<re2::RE2>;

    using ActionPtr = std::shared_ptr<IAction>;
    using Actions = std::vector<ActionPtr>;

    /** Шаблон. Может быть двух видов:
      * - подстрока:
      * -- в этом случае, substring_index != no_substring, и is_regexp == false.
      * -- для подстроки определяется точно, сматчена она или нет.
      * - регексп:
      * -- в этом случае, is_regexp == true; substring_index также может быть задан, если регексп требует наличия подстроки.
      * -- считается, что регексп "допустим", если подстрока не задана, или если подстрока сматчена.
      */
    struct Pattern
    {
        static const size_t no_substring = -1ULL;

        /// По этому индексу в массиве matched_substrings, можно узнать, сматчена ли подстрока.
        size_t substring_index;
        bool is_regexp;

        /** Для регекспа есть требуемая подстрока, и эта подстрока - префикс регекспа.
          * Используется для более быстрого (anchored) выполнения регекспа.
          */
        bool required_substring_is_prefix;

        std::string substring;
        std::string regexp;

        Pattern(size_t substring_index_, bool is_regexp_, bool required_substring_is_prefix_)
            : substring_index(substring_index_), is_regexp(is_regexp_), required_substring_is_prefix(required_substring_is_prefix_) {}
    };

    using Patterns = std::vector<Pattern>;


    /// Дерево условий для проверки
    struct Node
    {
        /** Набор шаблонов для проверки. Если хотя бы один шаблон сработал, то весь узел считается сработавшим.
          * Если хотя бы один regexp-шаблон "допустим", то следует проверить merged_regexp.
          */
        Patterns patterns;

        /** Регексп, который нужно проверить, если хотя бы один regexp-шаблон "допустим".
          */
        Regexp merged_regexp;
        UInt8 number_of_subpatterns = 0;

        bool only_one_regexp = false;
        UInt16 regexp_pattern_num = 0;

        std::string merged_regexp_str;
        std::string name;

        Actions actions;

        /// Список обычных детей. Если хотя бы один из них сработал, то остальные из этого списка проверять не надо.
        std::vector<Node> children_exclusive;

        /// Список детей, каждого из которых надо проверять независимо от того, сработали ли какие-либо другие.
        std::vector<Node> children_common;

        /// Список (из не более одного элемента) детей, которых надо проверить, если ни один из children_exclusive не сработал.
        std::vector<Node> children_default;
    };

    using Nodes = std::vector<Node>;

    void load(std::istream & browser_istr, std::istream & profile_istr, std::istream & extra_istr);

    void loadBrowsers(std::istream & istr);

    void addBranch(Poco::XML::Node & branch, Node & node);
    void addAction(Poco::XML::Node & define, const std::string & use_name, Actions & actions);
    void processMatch(Poco::XML::Node & match, Node & node);
    void processPattern(Poco::XML::Node & pattern, Node & node, std::string & regexp);

    void loadProfiles(std::istream & istr);

    void loadExtra(std::istream & istr);

    /// source_type - какая UserAgent строчка подаётся на вход
    template <SourceType source_type>
    void detectByUserAgent(const StringRef & user_agent_lower, Result & result, MatchedSubstrings & matched_substrings) const;

    /// source_type - какая UserAgent строчка подаётся на вход
    template <SourceType source_type>
    void detectByUserAgentCaseSafe(const StringRef & user_agent, const StringRef & user_agent_lower, Result & result, MatchedSubstrings & matched_substrings) const;

    void detectByProfile(StringRef profile, Result & result) const;
    static void fixResultAfterOpera(Result & result, const Result & opera_result);

    bool traverse(const Node & node, StringRef user_agent, Result & result, MatchedSubstrings & matched_substrings) const;

    using Strings = std::list<std::string>;
    Strings strings;

    Node root_node;

    size_t substrings_count = 0;
    std::unique_ptr<TDefaultAhoCorasickBuilder> automata_builder;
    std::unique_ptr<const TDefaultMappedAhoCorasick> automata;

    using SubstringsToIndices = std::map<std::string, size_t>;
    SubstringsToIndices substrings_to_indices;

    using Profiles = google::sparsehash::dense_hash_map<StringRef, Actions, StringRefHash>;
    Profiles profiles;

    /// Отображение DeviceModel -> DeviceName
    using ModelToName = google::sparsehash::dense_hash_map<std::string, std::string>;
    ModelToName model_to_name;


    std::string browser_path;
    std::string profiles_path;
    std::string extra_path;

    std::time_t browser_modification_time = 0;
    std::time_t profiles_modification_time = 0;
    std::time_t extra_modification_time = 0;

    Poco::Logger * log = &Poco::Logger::get("UATraits");

    std::unique_ptr<RootRule<Result>> root_rule;
};
