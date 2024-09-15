#include "AutocompleteModel.h"
#include <cassert>
#include <cstddef>
#include <string>
#include <vector>
#include <Client/TransformerModel.h>
#include <Parsers/Lexer.h>

std::string toUpperCaseString(const char *begin, const char *end) {
        std::string result;
        
        result.reserve(end - begin);

        for (const char *ptr = begin; ptr != end; ++ptr) {
            result += std::toupper(*ptr);
        }

        return result;
}

bool AutocompleteModel::isTokenIdentifier(const DB::Token& token) const {
    if (token.type == DB::TokenType::QuotedIdentifier) {
        return true;
    }
    if (token.type != DB::TokenType::BareWord) {
        return false;
    }
    std::string token_content_uppercase = toUpperCaseString(token.begin, token.end);
    if (keywords.contains(token_content_uppercase)) {
        return false;
    }
    return true;
}

bool AutocompleteModel::isTokenKeyword(const DB::Token& token) const {
    return (token.type == DB::TokenType::BareWord && !isTokenIdentifier(token));

}

bool AutocompleteModel::isTokenLiteral(const DB::Token& token) const {
    return (token.type == DB::TokenType::StringLiteral || token.type == DB::TokenType::Number || toUpperCaseString(token.begin, token.end) == "NULL");
}

bool AutocompleteModel::isTokenOperator(const DB::Token& prev_token, const DB::Token& token) const {
    if (token.type == DB::TokenType::Asterisk) {
        return isTokenIdentifier(prev_token) || isTokenLiteral(prev_token);
    }
    return operator_types.contains(token.type) || bare_words_operators.contains(toUpperCaseString(token.begin, token.end));
}

bool AutocompleteModel::isShortRec(const std::string& recomendation) const {
    return short_tokens.contains(recomendation);
}

bool AutocompleteModel::isBadRec(const std::string& rec) const {
    return isShortRec(rec) || rec == GPTJModel::unk || rec == GPTJModel::eos || rec == GPTJModel::bos || rec == GPTJModel::pad;
}

std::vector<std::string> AutocompleteModel::postprocessRecs(const std::vector<std::string>& raw_recs) const {
    std::vector<std::string> result;
    result.reserve(raw_recs.size());

    if (raw_recs.empty() || raw_recs[0] == GPTJModel::eos || raw_recs[0] == GPTJModel::bos || raw_recs[0] == GPTJModel::pad) {
        return result;
    }

    for (const auto& rec: raw_recs) {
        if (isBadRec(rec)) {
            continue;
        }
        if (rec == GPTJModel::literal && markov_literals.empty()) {
            continue;
        }
        if (rec == GPTJModel::identifier && markov_identifiers.empty()) {
            continue;
        }
        if (rec == GPTJModel::operator_token && markov_operators.empty()) {
            continue;
        }
        result.push_back(rec);
    }

    return result;
}

void AutocompleteModel::replaceWithMarkovPredictions(std::vector<std::string>& transformer_recs, std::span<const std::string> preprocessed_tokens_for_markov) const {
    if (transformer_recs.empty()) {
        return;
    }

    size_t max_predictions = 3;

    for (size_t i = 0; i < transformer_recs.size(); ++i) {
        std::vector<std::string> markov_predictions;

        size_t predictions_num = std::max(1, static_cast<int>(max_predictions - i));

        if (transformer_recs[i] == GPTJModel::literal && !markov_literals.empty()) {
            markov_predictions = markov_literals.predictNext(preprocessed_tokens_for_markov, predictions_num);
        } else if (transformer_recs[i] == GPTJModel::identifier && !markov_identifiers.empty()) {
            markov_predictions = markov_identifiers.predictNext(preprocessed_tokens_for_markov, predictions_num);
        } else if (transformer_recs[i] == GPTJModel::operator_token && !markov_operators.empty()) {
            markov_predictions = markov_operators.predictNext(preprocessed_tokens_for_markov, predictions_num);
        }

        // If we have Markov predictions, replace the transformer_rec with them
        if (!markov_predictions.empty()) {
            transformer_recs.erase(transformer_recs.begin() + i);
            transformer_recs.insert(transformer_recs.begin() + i, markov_predictions.begin(), markov_predictions.end());

            i += markov_predictions.size() - 1;
        }
    }
}

void AutocompleteModel::deleteDuplicatesKeepOrder(std::vector<std::string>& recs) const {
    std::unordered_set<std::string> seen;
    auto it = recs.begin();

    while (it != recs.end()) {
        if (seen.find(*it) != seen.end()) {
            it = recs.erase(it);
        } else {
            seen.insert(*it);
            ++it;
        }
    }
}

std::vector<std::string> AutocompleteModel::predictNextWords(DB::Lexer& lexer) {
    if (processed_queries_cnt < 1) {
        return {};
    }

    auto [preprocessed_for_tf, preprocessed_for_markov] = preprocessTokens(lexer);

    if (preprocessed_for_tf.empty() || preprocessed_for_markov.empty()) {
        return {};
    }

    auto recs = transformer_model.getRecsTopN(preprocessed_for_tf, recs_number);
    if (!markov_all.empty()) {
        auto [markov_all_rec, prob] = markov_all.predictNextWithProb(preprocessed_for_markov);

        if (prob > 0.8 && markov_all_rec.size() > 1) {
            recs.insert(recs.begin(),markov_all_rec); 
        }
    }

    recs = postprocessRecs(recs);
    replaceWithMarkovPredictions(recs, preprocessed_for_markov);
    deleteDuplicatesKeepOrder(recs);

    return recs;
}

void AutocompleteModel::addQuery(DB::Lexer& lexer) {
    if (markov_order == 0) {
        return;
    }
    auto [preprocessed_for_tf, preprocessed_for_markov] = preprocessTokens(lexer);
    
    if (preprocessed_for_tf.empty() || preprocessed_for_markov.empty()) {
        return;
    }

    for (size_t i = 1; i != markov_order; ++i) {
        preprocessed_for_markov.insert(preprocessed_for_markov.begin(), GPTJModel::bos);
        preprocessed_for_tf.insert(preprocessed_for_tf.begin(), GPTJModel::bos);
    }

    markov_all.addFullQuery(preprocessed_for_markov);

    for (size_t i = markov_order - 1; i != preprocessed_for_tf.size(); ++i) {
        if (preprocessed_for_tf[i] == GPTJModel::literal) {
            markov_literals.addExample(std::span<const std::string>(preprocessed_for_markov.begin() + (i + 1) - markov_order, preprocessed_for_markov.begin() + i + 1));
        }
        if (preprocessed_for_tf[i] == GPTJModel::identifier) {
            markov_identifiers.addExample(std::span<const std::string>(preprocessed_for_markov.begin() + (i + 1) - markov_order, preprocessed_for_markov.begin() + i + 1));
        }
        if (preprocessed_for_tf[i] == GPTJModel::operator_token) {
            markov_operators.addExample(std::span<const std::string>(preprocessed_for_markov.begin() + (i + 1) - markov_order, preprocessed_for_markov.begin() + i + 1));
        }
    }

    /// TODO: maybe increase only if the models have changed?
    markov_literals.incTimestamp();
    markov_identifiers.incTimestamp();
    markov_operators.incTimestamp();
    markov_all.incTimestamp();

    processed_queries_cnt++;

}

bool AutocompleteModel::isBareWordEqualToString(const DB::Token& token, const std::string& str) const {
    return token.type == DB::TokenType::BareWord && toUpperCaseString(token.begin, token.end) == str;
}


void AutocompleteModel::squashTokens(std::vector<DB::Token>& tokens, size_t start_index, size_t end_index, const std::string& operator_literal) const {
    auto it = bare_words_operators.find(operator_literal);
    if (it != bare_words_operators.end()) {
        const char* begin_replacement = it->c_str();
        const char* end_replacement = begin_replacement + std::strlen(begin_replacement);
        DB::Token new_token(DB::TokenType::BareWord, begin_replacement, end_replacement);

        tokens.erase(tokens.begin() + start_index, tokens.begin() + end_index + 1);

        tokens.insert(tokens.begin() + start_index, new_token);
    }
}



void AutocompleteModel::squashOperatorTokens(std::vector<DB::Token>& tokens) const {
    if (tokens.size() < 3) {
        return;
    }

    size_t initial_size = tokens.size();
    size_t replaced_2_cnt = 0;
    size_t replaced_3_cnt = 0;
    std::vector<std::string> after_not = {"BETWEEN", "IN", "LIKE", "EXISTS"};
    std::vector<std::string> before_not = {"AND", "OR"};

    for (size_t i = 1; i != tokens.size(); ++i) {
        
        if (i >= 2) {
            if (isBareWordEqualToString(tokens[i - 2], "GLOBAL") &&
                isBareWordEqualToString(tokens[i - 1], "NOT") &&
                isBareWordEqualToString(tokens[i], "IN")) {
                
                squashTokens(tokens, i - 2, i, "GLOBAL NOT IN");
                i -= 2;
                replaced_3_cnt++;
                continue;
            }
        }

        if (isBareWordEqualToString(tokens[i - 1], "GLOBAL") && isBareWordEqualToString(tokens[i], "IN")) {
            squashTokens(tokens, i - 1, i, "GLOBAL IN");
            i--;
            replaced_2_cnt++;
            continue;
        }

        for (const auto& word: after_not) {
            if (isBareWordEqualToString(tokens[i - 1], "NOT") && isBareWordEqualToString(tokens[i], word)) {
                std::string operator_literal = "NOT ";
                operator_literal += word;
                squashTokens(tokens, i - 1, i, operator_literal);
                i--;
                replaced_2_cnt++;
            }
        }

        for (const auto& word: before_not) {
            if (isBareWordEqualToString(tokens[i - 1], word) && isBareWordEqualToString(tokens[i], "NOT")) {
                std::string operator_literal = word;
                operator_literal += " NOT";
                squashTokens(tokens, i - 1, i, operator_literal);
                i--;
                replaced_2_cnt++;
            }
        }


        if (tokens[i - 1].type == DB::TokenType::Minus) {
            if (i >= 2 && !(isTokenIdentifier(tokens[i - 2]) || isTokenLiteral(tokens[i - 2]))) {
                
                // If the current token is a Number, squash the Minus and Number tokens
                if (tokens[i].type == DB::TokenType::Number) {
                    const char* begin_replacement = tokens[i - 1].begin;
                    const char* end_replacement = tokens[i].end;
                    DB::Token new_token(DB::TokenType::Number, begin_replacement, end_replacement);

                    tokens.erase(tokens.begin() + i - 1, tokens.begin() + i + 1);

                    tokens.insert(tokens.begin() + i - 1, new_token);

                    i--;
                    replaced_2_cnt++;
                }
            }
        }
    }
    assert(tokens.size() == initial_size - replaced_2_cnt - 2 * replaced_3_cnt);
}

std::vector<std::string> AutocompleteModel::preprocessForTransformer(const std::vector<DB::Token>& tokens) const {
    std::vector<std::string> result;
    result.reserve(tokens.size());
    for (size_t i = 0; i != tokens.size(); ++i) {
        if (i >= 1 && isTokenOperator(tokens[i - 1], tokens[i])) {
            result.push_back(GPTJModel::operator_token);
            continue;
        }
        if (isTokenLiteral(tokens[i])) {
            result.push_back(GPTJModel::literal);
            continue;
        }
        if (isTokenKeyword(tokens[i])) {
            result.push_back(toUpperCaseString(tokens[i].begin, tokens[i].end));
            continue;
        }
        if (isTokenIdentifier(tokens[i])) {
            result.push_back(GPTJModel::identifier);
            continue;
        }
        result.push_back(getTokenName(tokens[i].type));
    }
    assert(result.size() == tokens.size());
    return result;
}

std::vector<std::string> AutocompleteModel::preprocessForMarkov(const std::vector<DB::Token>& tokens) const {
    std::vector<std::string> result;
    result.reserve(tokens.size());
    for (const auto & token : tokens) {
        if (isTokenKeyword(token)) {
            result.push_back(toUpperCaseString(token.begin, token.end));
        } else {
            result.push_back(std::string(token.begin, token.end));
        }
    }
    assert(result.size() == tokens.size());
    return result;
}


std::pair<std::vector<std::string>, std::vector<std::string>> AutocompleteModel::preprocessTokens(DB::Lexer& lexer) const {
    std::vector<DB::Token> tokens_from_lexer{};

    while (true)
    {
        DB::Token token = lexer.nextToken();

        if (token.isEnd())
            break;

        if (token.isError())
            return {};

        if (!token.isSignificant())
            continue;
        
        tokens_from_lexer.push_back(token);
    }

    squashOperatorTokens(tokens_from_lexer);

    auto tokens_for_tf = preprocessForTransformer(tokens_from_lexer);
    auto tokens_for_markov = preprocessForMarkov(tokens_from_lexer);


    assert(tokens_for_tf.size() == tokens_for_markov.size());
    return {tokens_for_tf, tokens_for_markov};
}


const std::unordered_set<std::string> AutocompleteModel::bare_words_operators {
    "AND",
    "OR",
    "NOT",
    "AND NOT",
    "OR NOT",
    "IN",
    "NOT IN",
    "LIKE",
    "NOT LIKE",
    "BETWEEN",
    "NOT BETWEEN",
    "GLOBAL IN",
    "GLOBAL NOT IN",
    "EXISTS",
    "NOT EXISTS",
};

const std::unordered_set<DB::TokenType> AutocompleteModel::operator_types {
    // arithm
    DB::TokenType::Plus,
    DB::TokenType::Minus,
    DB::TokenType::Asterisk,
    DB::TokenType::Percent,

    // comparison
    DB::TokenType::Equals,
    DB::TokenType::NotEquals,
    DB::TokenType::GreaterOrEquals,
    DB::TokenType::LessOrEquals,
    DB::TokenType::Less,
    DB::TokenType::Greater,
    DB::TokenType::Spaceship,
};


const std::unordered_set<std::string> AutocompleteModel::short_tokens {
    DB::getTokenName(DB::TokenType::Equals),
    DB::getTokenName(DB::TokenType::LessOrEquals),
    DB::getTokenName(DB::TokenType::GreaterOrEquals),
    DB::getTokenName(DB::TokenType::Less),
    DB::getTokenName(DB::TokenType::Greater),
    DB::getTokenName(DB::TokenType::NotEquals),
    DB::getTokenName(DB::TokenType::OpeningRoundBracket),
    DB::getTokenName(DB::TokenType::ClosingRoundBracket),
    DB::getTokenName(DB::TokenType::OpeningSquareBracket),
    DB::getTokenName(DB::TokenType::ClosingSquareBracket),
    DB::getTokenName(DB::TokenType::OpeningCurlyBrace),
    DB::getTokenName(DB::TokenType::ClosingCurlyBrace),
    DB::getTokenName(DB::TokenType::Comma),
    DB::getTokenName(DB::TokenType::Semicolon),
    DB::getTokenName(DB::TokenType::VerticalDelimiter),
    DB::getTokenName(DB::TokenType::Dot),
    DB::getTokenName(DB::TokenType::Asterisk),
    DB::getTokenName(DB::TokenType::Slash),
    DB::getTokenName(DB::TokenType::Plus),
    DB::getTokenName(DB::TokenType::Minus),
    DB::getTokenName(DB::TokenType::Percent),
    DB::getTokenName(DB::TokenType::Arrow),
    DB::getTokenName(DB::TokenType::QuestionMark),
    DB::getTokenName(DB::TokenType::Colon),
    DB::getTokenName(DB::TokenType::DoubleColon),
    DB::getTokenName(DB::TokenType::Spaceship),
    DB::getTokenName(DB::TokenType::PipeMark),
    DB::getTokenName(DB::TokenType::Concatenation),
    DB::getTokenName(DB::TokenType::At),
    DB::getTokenName(DB::TokenType::DoubleAt),
};

/// TODO: remove function names
const std::unordered_set<std::string> AutocompleteModel::keywords = {
    "ACCESS",
    "ACTION",
    "ADD",
    "ADMIN",
    "AFTER",
    "ALGORITHM",
    "ALIAS",
    "ALL",
    "ALLOWED_LATENESS",
    "ALTER",
    "ANTI",
    "ANY",
    "APPLY",
    "ARRAY",
    "AS",
    "ASC",
    "ASCENDING",
    "ASOF",
    "ASSUME",
    "AST",
    "ASYNC",
    "ATTACH",
    "AUTO_INCREMENT",
    "BACKUP",
    "BASE_BACKUP",
    "BEGIN",
    "BIDIRECTIONAL",
    "BOTH",
    "BY",
    "CACHE",
    "CACHES",
    "CASCADE",
    "CASE",
    "CASEWITHEXPRESSION",
    "CAST",
    "CHANGE",
    "CHANGEABLE_IN_READONLY",
    "CHANGED",
    "CHAR",
    "CHARACTER",
    "CHECK",
    "CLEANUP",
    "CLEAR",
    "CLUSTER",
    "CLUSTER_HOST_IDS",
    "CLUSTERS",
    "CN",
    "CODEC",
    "COLLATE",
    "COLLECTION",
    "COLUMN",
    "COLUMNS",
    "COMMENT",
    "COMMIT",
    "COMPRESSION",
    "CONCAT",
    "CONSTRAINT",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "CURRENT_USER",
    "DATABASE",
    "DATABASES",
    "DATE",
    "DATE_ADD",
    "DATEADD",
    "DATE_DIFF",
    "DATEDIFF",
    "DATE_SUB",
    "DATESUB",
    "DAY",
    "DD",
    "DDL",
    "DEDUPLICATE",
    "DEFAULT",
    "DELAY",
    "DELETE",
    "DESC",
    "DESCENDING",
    "DESCRIBE",
    "DETACH",
    "DETACHED",
    "DICTIONARIES",
    "DICTIONARY",
    "DISK",
    "DISTINCT",
    "DIV",
    "DOUBLE_SHA1_HASH",
    "DROP",
    "ELSE",
    "EMPTY",
    "ENABLED",
    "END",
    "ENFORCED",
    "ENGINE",
    "EPHEMERAL",
    "EQUALS",
    "ESTIMATE",
    "EVENT",
    "EVENTS",
    "EXCEPT",
    "EXCHANGE",
    "EXISTS",
    "EXPLAIN",
    "EXPRESSION",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FILE",
    "FILESYSTEM",
    "FILL",
    "FILTER",
    "FINAL",
    "FIRST",
    "FOLLOWING",
    "FOR",
    "FOREIGN",
    "FORMAT",
    "FREEZE",
    "FROM",
    "FULL",
    "FULLTEXT",
    "FUNCTION",
    "GRANT",
    "GRANTEES",
    "GRANTS",
    "GRANULARITY",
    "GREATER",
    "GREATEROREQUALS",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HASH",
    "HAVING",
    "HDFS",
    "HH",
    "HIERARCHICAL",
    "HOST",
    "HOUR",
    "ID",
    "IDENTIFIED",
    "IF",
    "ILIKE",
    "IN",
    "INDEX",
    "INFILE",
    "INHERIT",
    "INJECTIVE",
    "INNER",
    "INSERT",
    "INTERPOLATE",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVISIBLE",
    "IP",
    "IS",
    "IS_OBJECT_ID",
    "JOIN",
    "KEY",
    "KEYED",
    "KILL",
    "LAMBDA",
    "LARGE",
    "LAST",
    "LAYOUT",
    "LEADING",
    "LEFT",
    "LESS",
    "LESSOREQUALS",
    "LEVEL",
    "LIFETIME",
    "LIMIT",
    "LIMITS",
    "LINEAR",
    "LIST",
    "LITERAL",
    "LIVE",
    "LOCAL",
    "LTRIM",
    "MATCH",
    "MATERIALIZE",
    "MATERIALIZED",
    "MCS",
    "MEMORY",
    "MI",
    "MICROSECOND",
    "MILLISECOND",
    "MINUS",
    "MINUTE",
    "MM",
    "MOD",
    "MODIFY",
    "MONTH",
    "MOVE",
    "MS",
    "MULTIIF",
    "MUTATION",
    "NAMED",
    "NANOSECOND",
    "NEXT",
    "NO",
    "NONE",
    "NOTEQUALS",
    "NOTIN",
    "NS",
    "NULLS",
    "OBJECT",
    "OFFSET",
    "ON",
    "ONLY",
    "OPTIMIZE",
    "OPTION",
    "ORDER",
    "OUTER",
    "OUTFILE",
    "OVER",
    "OVERRIDE",
    "PART",
    "PARTIAL",
    "PARTITION",
    "PARTITIONS",
    "PART_MOVE_TO_SHARD",
    "PERMANENTLY",
    "PERMISSIVE",
    "PIPELINE",
    "PLAN",
    "PLUS",
    "POLICY",
    "POPULATE",
    "POSITION",
    "PRECEDING",
    "PRECISION",
    "PREWHERE",
    "PRIMARY",
    "PRIVILEGES",
    "PROCESSLIST",
    "PROFILE",
    "PROJECTION",
    "QQ",
    "QUARTER",
    "QUERY",
    "QUOTA",
    "RANDOMIZED",
    "RANGE",
    "READONLY",
    "REALM",
    "RECOMPRESS",
    "REFERENCES",
    "REFRESH",
    "REGEXP",
    "REGEXPQUOTEMETA",
    "REMOVE",
    "RENAME",
    "REPLACE",
    "REPLACEREGEXPALL",
    "REPLACEREGEXPONE",
    "RESET",
    "RESTORE",
    "RESTRICT",
    "RESTRICTIVE",
    "RESUME",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLES",
    "ROLLBACK",
    "ROLLUP",
    "ROW",
    "ROWS",
    "RTRIM",
    "S3",
    "SALT",
    "SAMPLE",
    "SECOND",
    "SELECT",
    "SEMI",
    "SERVER",
    "SET",
    "SETS",
    "SETTING",
    "SETTINGS",
    "SHA256_HASH",
    "SHARD",
    "SHOW",
    "SIGNED",
    "SIMPLE",
    "SINGLEVALUEORNULL",
    "SNAPSHOT",
    "SOURCE",
    "SPATIAL",
    "SS",
    "STDOUT",
    "STEP",
    "STORAGE",
    "STRICT",
    "STRICTLY_ASCENDING",
    "SUBPARTITION",
    "SUBPARTITIONS",
    "SUBSTRING",
    "SUSPEND",
    "SYNC",
    "SYNTAX",
    "TABLE",
    "TABLES",
    "TEMPORARY",
    "TEST",
    "THAN",
    "THEN",
    "TIES",
    "TIMESTAMP",
    "TIMESTAMP_ADD",
    "TIMESTAMPADD",
    "TIMESTAMP_DIFF",
    "TIMESTAMPDIFF",
    "TIMESTAMP_SUB",
    "TIMESTAMPSUB",
    "TO",
    "TODATE",
    "TODATETIME",
    "TOP",
    "TOTALS",
    "TRACKING",
    "TRAILING",
    "TRANSACTION",
    "TREE",
    "TRIGGER",
    "TRIM",
    "TRIMBOTH",
    "TRIMLEFT",
    "TRIMRIGHT",
    "TRUE",
    "TRUNCATE",
    "TTL",
    "TUPLE",
    "TYPE",
    "UNBOUNDED",
    "UNFREEZE",
    "UNION",
    "UNIQUE",
    "UNSIGNED",
    "UNTUPLE",
    "UPDATE",
    "URL",
    "USE",
    "USER",
    "USING",
    "UUID",
    "VALUES",
    "VARYING",
    "VIEW",
    "VIEWIFPERMITTED",
    "VISIBLE",
    "VOLUME",
    "WATCH",
    "WATERMARK",
    "WEEK",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WK",
    "WRITABLE",
    "YEAR",
    "YYYY",
    "ZKPATH",
    "AND",
    "OR",
    "NOT",
    "BETWEEN",
    "GLOBAL",
    "IN",
    "LIKE",
    "EXISTS",
    "NULL",
};
