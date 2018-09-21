#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <TableFunctions/TableFunctionRemote.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


/// The Cartesian product of two sets of rows, the result is written in place of the first argument
static void append(std::vector<String> & to, const std::vector<String> & what, size_t max_addresses)
{
    if (what.empty())
        return;

    if (to.empty())
    {
        to = what;
        return;
    }

    if (what.size() * to.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
    std::vector<String> res;
    for (size_t i = 0; i < to.size(); ++i)
        for (size_t j = 0; j < what.size(); ++j)
            res.push_back(to[i] + what[j]);

    to.swap(res);
}


/// Parse number from substring
static bool parseNumber(const String & description, size_t l, size_t r, size_t & res)
{
    res = 0;
    for (size_t pos = l; pos < r; pos ++)
    {
        if (!isNumericASCII(description[pos]))
            return false;
        res = res * 10 + description[pos] - '0';
        if (res > 1e15)
            return false;
    }
    return true;
}



/* Parse a string that generates shards and replicas. Separator - one of two characters | or ,
 *  depending on whether shards or replicas are generated.
 * For example:
 * host1,host2,...      - generates set of shards from host1, host2, ...
 * host1|host2|...      - generates set of replicas from host1, host2, ...
 * abc{8..10}def        - generates set of shards abc8def, abc9def, abc10def.
 * abc{08..10}def       - generates set of shards abc08def, abc09def, abc10def.
 * abc{x,yy,z}def       - generates set of shards abcxdef, abcyydef, abczdef.
 * abc{x|yy|z} def      - generates set of replicas abcxdef, abcyydef, abczdef.
 * abc{1..9}de{f,g,h}   - is a direct product, 27 shards.
 * abc{1..9}de{0|1}     - is a direct product, 9 shards, in each 2 replicas.
 */
static std::vector<String> parseDescription(const String & description, size_t l, size_t r, char separator, size_t max_addresses)
{
    std::vector<String> res;
    std::vector<String> cur;

    /// An empty substring means a set of an empty string
    if (l >= r)
    {
        res.push_back("");
        return res;
    }

    for (size_t i = l; i < r; ++i)
    {
        /// Either the numeric interval (8..10) or equivalent expression in brackets
        if (description[i] == '{')
        {
            int cnt = 1;
            int last_dot = -1; /// The rightmost pair of points, remember the index of the right of the two
            size_t m;
            std::vector<String> buffer;
            bool have_splitter = false;

            /// Look for the corresponding closing bracket
            for (m = i + 1; m < r; ++m)
            {
                if (description[m] == '{') ++cnt;
                if (description[m] == '}') --cnt;
                if (description[m] == '.' && description[m-1] == '.') last_dot = m;
                if (description[m] == separator) have_splitter = true;
                if (cnt == 0) break;
            }
            if (cnt != 0)
                throw Exception("Table function 'remote': incorrect brace sequence in first argument",
                                ErrorCodes::BAD_ARGUMENTS);
            /// The presence of a dot - numeric interval
            if (last_dot != -1)
            {
                size_t left, right;
                if (description[last_dot - 1] != '.')
                    throw Exception("Table function 'remote': incorrect argument in braces (only one dot): " + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, i + 1, last_dot - 1, left))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect left number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (!parseNumber(description, last_dot + 1, m, right))
                    throw Exception("Table function 'remote': incorrect argument in braces (Incorrect right number): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (left > right)
                    throw Exception("Table function 'remote': incorrect argument in braces (left number is greater then right): "
                                    + description.substr(i, m - i + 1),
                                    ErrorCodes::BAD_ARGUMENTS);
                if (right - left + 1 >  max_addresses)
                    throw Exception("Table function 'remote': first argument generates too many result addresses",
                        ErrorCodes::BAD_ARGUMENTS);
                bool add_leading_zeroes = false;
                size_t len = last_dot - 1 - (i + 1);
                 /// If the left and right borders have equal numbers, then you must add leading zeros.
                if (last_dot - 1 - (i + 1) == m - (last_dot + 1))
                    add_leading_zeroes = true;
                for (size_t id = left; id <= right; ++id)
                {
                    String cur = toString<UInt64>(id);
                    if (add_leading_zeroes)
                    {
                        while (cur.size() < len)
                            cur = "0" + cur;
                    }
                    buffer.push_back(cur);
                }
            }
            else if (have_splitter) /// If there is a current delimiter inside, then generate a set of resulting rows
                buffer = parseDescription(description, i + 1, m, separator, max_addresses);
            else                     /// Otherwise just copy, spawn will occur when you call with the correct delimiter
                buffer.push_back(description.substr(i, m - i + 1));
            /// Add all possible received extensions to the current set of lines
            append(cur, buffer, max_addresses);
            i = m;
        }
        else if (description[i] == separator)
        {
            /// If the delimiter, then add found rows
            res.insert(res.end(), cur.begin(), cur.end());
            cur.clear();
        }
        else
        {
            /// Otherwise, simply append the character to current lines
            std::vector<String> buffer;
            buffer.push_back(description.substr(i, 1));
            append(cur, buffer, max_addresses);
        }
    }

    res.insert(res.end(), cur.begin(), cur.end());
    if (res.size() > max_addresses)
        throw Exception("Table function 'remote': first argument generates too many result addresses",
            ErrorCodes::BAD_ARGUMENTS);

    return res;
}


StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & ast_function, const Context & context) const
{
    ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

    const size_t max_args = is_cluster_function ? 3 : 5;
    if (args.size() < 2 || args.size() > max_args)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String cluster_name;
    String cluster_description;
    String remote_database;
    String remote_table;
    ASTPtr remote_table_function_ptr;
    String username;
    String password;

    size_t arg_num = 0;

    auto getStringLiteral = [](const IAST & node, const char * description)
    {
        const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(&node);
        if (!lit)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        if (lit->value.getType() != Field::Types::String)
            throw Exception(description + String(" must be string literal (in single quotes)."), ErrorCodes::BAD_ARGUMENTS);

        return safeGet<const String &>(lit->value);
    };

    if (is_cluster_function)
    {
        ASTPtr ast_name = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
        cluster_name = static_cast<const ASTLiteral &>(*ast_name).value.safeGet<const String &>();
    }
    else
    {
        if (auto ast_cluster = typeid_cast<const ASTIdentifier *>(args[arg_num].get()))
            cluster_name = ast_cluster->name;
        else
            cluster_description = getStringLiteral(*args[arg_num], "Hosts pattern");
    }
    ++arg_num;

    args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);

    const auto function = typeid_cast<const ASTFunction *>(args[arg_num].get());

    if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
    {
        remote_table_function_ptr = args[arg_num];
        ++arg_num;
    }
    else
    {
        remote_database = static_cast<const ASTLiteral &>(*args[arg_num]).value.safeGet<String>();

        ++arg_num;

        size_t dot = remote_database.find('.');
        if (dot != String::npos)
        {
            /// NOTE Bad - do not support identifiers in backquotes.
            remote_table = remote_database.substr(dot + 1);
            remote_database = remote_database.substr(0, dot);
        }
        else
        {
            if (arg_num >= args.size())
            {
                throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }
            else
            {
                args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                remote_table = static_cast<const ASTLiteral &>(*args[arg_num]).value.safeGet<String>();
                ++arg_num;
            }
        }
    }

    /// Username and password parameters are prohibited in cluster version of the function
    if (!is_cluster_function)
    {
        if (arg_num < args.size())
        {
            username = getStringLiteral(*args[arg_num], "Username");
            ++arg_num;
        }
        else
            username = "default";

        if (arg_num < args.size())
        {
            password = getStringLiteral(*args[arg_num], "Password");
            ++arg_num;
        }
    }

    if (arg_num < args.size())
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// ExpressionAnalyzer will be created in InterpreterSelectQuery that will meet these `Identifier` when processing the request.
    /// We need to mark them as the name of the database or table, because the default value is column.
    for (auto & arg : args)
        if (ASTIdentifier * id = typeid_cast<ASTIdentifier *>(arg.get()))
            id->setSpecial();

    ClusterPtr cluster;
    if (!cluster_name.empty())
    {
        /// Use an existing cluster from the main config
        cluster = context.getCluster(cluster_name);
    }
    else
    {
        /// Create new cluster from the scratch
        size_t max_addresses = context.getSettingsRef().table_function_remote_max_addresses;
        std::vector<String> shards = parseDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        std::vector<std::vector<String>> names;
        for (size_t i = 0; i < shards.size(); ++i)
            names.push_back(parseDescription(shards[i], 0, shards[i].size(), '|', max_addresses));

        if (names.empty())
            throw Exception("Shard list is empty after parsing first argument", ErrorCodes::BAD_ARGUMENTS);

        cluster = std::make_shared<Cluster>(context.getSettings(), names, username, password, context.getTCPPort(), false);
    }

    auto structure_remote_table = getStructureOfRemoteTable(*cluster, remote_database, remote_table, context, remote_table_function_ptr);

    StoragePtr res = remote_table_function_ptr
        ? StorageDistributed::createWithOwnCluster(
            getName(),
            structure_remote_table,
            remote_table_function_ptr,
            cluster,
            context)
        : StorageDistributed::createWithOwnCluster(
            getName(),
            structure_remote_table,
            remote_database,
            remote_table,
            cluster,
            context);

    res->startup();
    return res;
}


TableFunctionRemote::TableFunctionRemote(const std::string & name_)
    : name(name_)
{
    is_cluster_function = name == "cluster";

    std::stringstream ss;
    ss << "Table function '" << name + "' requires from 2 to " << (is_cluster_function ? 3 : 5) << " parameters"
       << ": <addresses pattern or cluster name>, <name of remote database>, <name of remote table>"
       << (is_cluster_function ? "" : ", [username, [password]].");
    help_message = ss.str();
}


void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); });
    factory.registerFunction("cluster", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("cluster"); });
}

}
