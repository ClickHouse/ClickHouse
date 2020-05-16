#include <iostream>

#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/parseQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTIdentifier.cpp>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWithAlias.h>


std::pair<std::string, std::string> get_table_a_column(std::string c) {
    auto point_place = c.rfind('.');
    std::string db = "";
    std::string column = "";
    if (point_place != std::string::npos) {
        db = c.substr(0, point_place);
        column = c.substr(point_place + 1);
    } else {
        column = c;
    }
    return {db, column};
}

class Table
{
public:

    bool column_exists(std::string column_name) {
        return columns.count(column_name) || columns_maybe.count(column_name);
    }

    void add_column(std::string column_name) {
        columns.insert(column_name);
    }

    void add_column_maybe(std::string column_name) {
        columns_maybe.insert(column_name);
    }

    std::string name;
    std::set<std::string> columns;
    std::set<std::string> columns_maybe;

};

class TableList
{
public:
    std::unordered_map<std::string, Table> tables;
    std::map<std::string, std::set<std::string>> columns_maybe;

    Table& get_table(std::string table_name) {
        auto table = tables[table_name];
        return table;
    }

    bool table_exists(std::string table_name) {
        return (tables.find(table_name) != tables.end());
    }

//    void add_column(std::string table_name, std::string column_name) {
//        auto table = tables[table_name];
//        table.columns.insert(column_name);
//    }

    void add_column(std::string column, std::set<std::string> column_tables, bool nested = false) {
        if (!nested) {
            std::string _;
            std::tie(_, column) = get_table_a_column(column);
        }
        if (column_tables.size() == 1) {
            tables[*column_tables.begin()].add_column(column);
        }
        else if (column_tables.size() > 1) {
            for (auto column_table : column_tables) {
                if (!tables[column_table].column_exists(column)) {
                    tables[column_table].add_column_maybe(column);
                    columns_maybe[column].insert(column_table);
                }
            }
        }
    }

    void add_table(std::string table_name) {
        Table table;
        table.name = table_name;
        tables[table_name] = table;

    }

    std::vector<Table> get_all_tables() {
        std::vector<Table> t;
        std::transform(tables.begin(), tables.end(), std::back_inserter(t),
                       [](const std::pair<std::string, Table> &mapItem)
                       {
                           return mapItem.second;
                       });
        return t;
    }

    void print(){
        for (auto table_d = tables.begin(); table_d != tables.end(); ++table_d) {
            std::cout << "Table\n";
            auto table = table_d->second;
            std::cout << table.name << "\n";
            std::cout << "Columns:\n";
            for (auto column : table.columns) {
                std::cout << column << " ";
            }
            std::cout << "\n";
            std::cout << "Columns_maybe:\n";
            for (auto column : table.columns_maybe) {
                std::cout << column << " ";
            }
            std::cout << "\n\n";
        }
    }
};

class returnTable
{
public:
    returnTable(){}

    returnTable(std::string table_name)
    {
        tables = {table_name};
        columns["*"] = {table_name};
    }

    void add_column(std::string column_name, std::set<std::string> table_names) {
        std::string _;
        std::tie(_, column_name) = get_table_a_column(column_name);
        columns[column_name].insert(table_names.begin(), table_names.end());
        tables.insert(table_names.begin(), table_names.end());
    }

    void add_column_aliases(std::string alias, std::set<std::string> column_names) {
        std::string _;
        column_aliases[alias] = {};
        for (auto column : column_names) {
            std::tie(_, column) = get_table_a_column(column);
            column_aliases[alias].insert(column);
        }
    }

    std::set<std::string> tables_by_columns(std::string column_name, bool nested=false) {
        std::set<std::string> result = {};
        std::string table, column;
        if (!nested) {
            std::tie(table, column) = get_table_a_column(column_name);
        }
        if (table != "") {
            if (columns.find(column) != columns.end() && columns[column].find(table) != columns[column].end()) {
                return {table};
            }
//            if (column_aliases.find(column) != column_aliases.end() && column_aliases[column].find(table) != column_aliases[column].end()) {
//                return {table};
//            }
            if(columns.find("*") != columns.end() && columns["*"].find(table) != columns["*"].end()) {
                return {table};
            }
        }
        else {
            if (columns.find(column) != columns.end()) {
                result.insert(columns[column].begin(), columns[column].end());
            }
            if (columns.find("*") != columns.end()) {
                result.insert(columns["*"].begin(), columns["*"].end());
            }
    //        if (column_aliases.find(column) != column_aliases.end()) {
    //            result.insert(column_aliases[column].begin(), column_aliases[column].end());
    //        }
        }
        return result;
    }

    void print() {
        std::cout << "Tables\n";
        for (auto t : tables) {
            std::cout << t << " ";
        }
        std::cout << "\nColumns\n";
        for (auto c = columns.begin(); c != columns.end(); ++c) {
            std::cout << c->first << ": ";
            int i = 1;
            for (auto t: c->second) {
                std::cout << i << ") " << t << ' ';
                i++;
            }
            std::cout << "\n";
        }
        std::cout << "\nColumn aliases\n";
        for (auto c = column_aliases.begin(); c != column_aliases.end(); ++c) {
            std::cout << c->first << ": ";
            int i = 1;
            for (auto t: c->second) {
                std::cout << i << ") " << t << ' ';
                i++;
            }
            std::cout << "\n";
        }
        std::cout << "\n";
    }

    std::set<std::string> tables;
    std::map<std::string, std::set<std::string>> columns;
    std::map<std::string, std::set<std::string>> column_aliases;
//    std::map<std::string, std::string> table_aliases;
};



std::string print_alias(DB::ASTPtr ch) {
    auto X = std::dynamic_pointer_cast<DB::ASTWithAlias>(ch);
    if (X) {
        return X->alias;
    }

    for (const auto & child: (*ch).children) {
        auto alias = print_alias(child);
        if (alias != "")
            return alias;
    }
    return "";

}

std::set<std::string> get_indent(DB::ASTPtr ch){
    if (!ch) {
        return {};
    }
    std::set<std::string> ret = {};
    auto X = std::dynamic_pointer_cast<DB::ASTIdentifier>(ch);
    if (X) {
        ret.insert(X->name);
    }
    for (const auto & child: (*ch).children) {
        auto child_ind = get_indent(child);
        ret.insert(child_ind.begin(), child_ind.end());
                if (child_ind.size() == 1) {
                    formatAST(*ch, std::cerr);
                    std::cout << "\n\n" << std::endl;
                }
    }
    return ret;

}

std::set<std::string> get_select_indent(DB::ASTPtr asp, std::map<std::string, std::set<std::string>>& column_alias){
    std::set<std::string> ret = {};
    for (auto ch : asp->children) {
        auto alias = print_alias(ch);
        auto columns = get_indent(ch);
        if (alias != "") {
            column_alias[alias].insert(columns.begin(), columns.end());
        }
        ret.insert(columns.begin(), columns.end());
    }
    return ret;

}

void print_tables(DB::ASTPtr ch) {
    auto X = std::dynamic_pointer_cast<DB::ASTTableExpression>(ch);
    if (X) {
//        auto tables = (*X).tables();
//        print_indent(tables);
        auto Y = (*X).database_and_table_name;
//        formatAST(*Y, std::cerr);
//        print_indent(ch);
//        std::cout << "\n";
        print_alias(ch);
//        std::cout << std::endl;
    }

    for (const auto & child: (*ch).children) {
        print_tables(child);
    }

}

void select_query(DB::ASTPtr ch, int depth) {
    auto X = std::dynamic_pointer_cast<DB::ASTSelectQuery>(ch);
//    if (X) {
//        std::cout << depth << " found\n";
//    }

    for (const auto & child: (*ch).children) {
        select_query(child, depth+1);
    }

}

std::vector<DB::ASTPtr> get_select(DB::ASTPtr vertex) {
    auto X = std::dynamic_pointer_cast<DB::ASTSelectQuery>(vertex);
    std::vector<DB::ASTPtr> result;
    if (X) {
        result.push_back(vertex);
        return result;
    }

    for (const auto & child: (*vertex).children) {
        auto v = get_select(child);
        result.insert(result.end(), v.begin(), v.end());
    }
    return result;
}
std::set<std::string> get_all_tables_from_return(std::string full_column, std::vector<returnTable> returned_tables, std::map<std::string, std::set<int>> table_aliases) {
    std::string table, column;
    std::tie(table, column) = get_table_a_column(full_column);
    std::set<std::string> result = {};
    if (table_aliases.count(table)) {
        for (auto ret_num: table_aliases[table]) {
            auto t = returned_tables[ret_num].tables_by_columns(column);
            result.insert(t.begin(), t.end());
        }
    }
    for(auto ret_table: returned_tables) {
        auto t = ret_table.tables_by_columns(full_column);
//        std::cout << full_column;
//        for (auto _: t) std::cout << " " << _;
//        std::cout <<'\n';
        result.insert(t.begin(), t.end());
    }
    return result;
}

std::set<std::string> get_all_nested_tables_from_return(std::string full_column, std::vector<returnTable> returned_tables) {
    std::set<std::string> result = {};
    for(auto ret_table: returned_tables) {
        auto t = ret_table.tables_by_columns(full_column, true);
        result.insert(t.begin(), t.end());
    }
    return result;
}

bool is_alias(std::string full_column, std::vector<returnTable> returned_tables) {
    std::string table, column;
    std::tie(table, column) = get_table_a_column(full_column);
    std::set<std::string> result = {};
    for(auto ret_table: returned_tables) {
        if (ret_table.column_aliases.count(column) || ret_table.column_aliases.count(full_column)) {
            return true;
        }
    }
    return false;
}

returnTable parse_sql(DB::ASTPtr ast, TableList& all_tables) {
    auto sast = std::dynamic_pointer_cast<DB::ASTSelectQuery>(ast);
    if (!sast) {
        std::cerr << "not select query";
        return returnTable();
    }
    returnTable result;
    std::set<std::string> columns = {};
    std::set<std::string> tables = {};
    std::map<std::string, std::set<int>> table_aliases = {};
    std::map<std::string, std::set<std::string>> column_alias = {};

    std::vector<returnTable> now_tables;
    auto X = sast->tables();
    for (auto child : X->children) {
        auto ch = std::dynamic_pointer_cast<DB::ASTTablesInSelectQueryElement>(child);
        auto TEast = std::dynamic_pointer_cast<DB::ASTTableExpression>(ch->table_expression);
        if (TEast && TEast->database_and_table_name) {
            auto table_name = *(get_indent(TEast->database_and_table_name).begin());
            tables.insert(table_name);
            now_tables.push_back(returnTable(table_name));
            all_tables.add_table(table_name);
            auto alias = print_alias(ch);
            if (alias != "") {
                table_aliases[alias].insert(now_tables.size() - 1);
            }
        }
        if (TEast && TEast->subquery) {
//            std::cout << "subselect\n";
            auto sub_select_vector = get_select(TEast->subquery);
            auto alias = print_alias(ch);
            for (auto sub_select: sub_select_vector) {
                now_tables.push_back(parse_sql(sub_select, all_tables));
                if (alias != "") {
                    table_aliases[alias].insert(now_tables.size() - 1);
                }
            }
        }


        if (ch->table_join) {
            auto jch = std::dynamic_pointer_cast<DB::ASTTableJoin>(ch->table_join);
            if (jch->using_expression_list)
            {
                auto join_columns = get_indent(jch->using_expression_list);
                columns.insert(join_columns.begin(), join_columns.end());
            }
            else if (jch->on_expression)
            {
                auto join_columns = get_indent(jch->on_expression);
                columns.insert(join_columns.begin(), join_columns.end());
            }
        }
    }

    auto select_columns = get_select_indent(sast->select(), column_alias);
    columns.insert(select_columns.begin(), select_columns.end());
    if(select_columns.empty()) select_columns.insert("*");

    for (auto column : select_columns) {
        auto t = get_all_tables_from_return(column, now_tables, table_aliases);
//        std::cout << column;
//        for (auto _ : t) std::cout << " " <<_;
//        std::cout <<"\n";
        result.add_column(column, t);
    }

    for (auto s = column_alias.begin(); s != column_alias.end(); ++s) {
        result.add_column_aliases(s->first, s->second);
    }

    auto where_columns = get_indent(sast->where());
    columns.insert(where_columns.begin(), where_columns.end());

//    auto groupby_columns = get_indent(sast->groupBy());
//    columns.insert(groupby_columns.begin(), groupby_columns.end());
//
//    auto orderby_columns = get_indent(sast->orderBy());
//    columns.insert(orderby_columns.begin(), orderby_columns.end());

    auto having_columns = get_indent(sast->having());
    columns.insert(having_columns.begin(), having_columns.end());

    for (auto column : columns) {
        if (is_alias(column, now_tables)) {
            continue;
        }
        auto t = get_all_tables_from_return(column, now_tables, table_aliases);
//        std::cout << column << " ";
//        for (auto table: t)
//            std::cout << table <<" ";
//        std::cout << "\n";
        if (!t.empty()) {
            all_tables.add_column(column, t);
        } else {
            t = get_all_nested_tables_from_return(column, now_tables);
            all_tables.add_column(column, t, true);
        }
    }


//    std::cout << "\ncolumns:\n";
//    for (auto col : columns)
//        std::cout << col << "\n";
//
//    std::cout << "\ntables:\n";
//    for (auto tab : tables)
//        std::cout << tab << "\n";
    result.print();
    return result;
}

int main(int, char **)
try
{
    using namespace DB;

    std::string input2 =
    "        SELECT "
    "CounterID, "
    "        hits, "
    "        visits "
    "FROM "
    "       ( "
    "                SELECT "
    "CounterID, "
    "        count() AS hits "
    "FROM test.hits "
    "GROUP BY CounterID "
    ") ANY LEFT JOIN "
    "        ( "
    "                SELECT "
    "                CounterID, "
    "                sum(Sign) AS visits "
    "FROM test.visits "
    "GROUP BY CounterID "
    ") USING CounterID "
    "ORDER BY lel1 DESC "
    "LIMIT 1";

    std::string input =
        " SELECT 18446744073709551615 as some_number, f(1), '\\\\', [a, b, c], (a, b, c), 1 + 2 * -3, a = b OR c > d.1 + 2 * -g[0] AND NOT e < f * (x + y), kek.lol"
        " FROM default.hits"
        " LEFT JOIN default.kek as kek"
        " ON default.hits.kekos = default.kek.kekosik"
        " WHERE CounterID = 101500 AND UniqID % 3 = 0"
        " GROUP BY UniqID"
        " HAVING SUM(Refresh) > 100"
        " ORDER BY Visits, PageViews"
        " LIMIT LENGTH('STRING OF 20 SYMBOLS') - 20 + 1000, 10.05 / 5.025 * 5"
        " INTO OUTFILE 'test.out'"
        " FORMAT TabSeparated";

    std::string simple =
            " SELECT 18446744073709551615, f(1), '\\\\',kek.lol, [a, b, c], (a, b, c), 1 + 2 * -3, a = b OR c > d.1 + 2 * -g[0] AND NOT e < f * (x + y)"
            " FROM default.hits"
            " LEFT JOIN default.kek as kek"
            " ON default.hits.kekos = default.kek.kekosik"
            " WHERE CounterID = 101500 AND UniqID % 3 = 0";

    std::string input3 = "SELECT val FROM (SELECT value AS val FROM data2013 WHERE name = 'Alice' UNION /*comment*/ ALL SELECT value AS val FROM data2014 WHERE name = 'Alice') ORDER BY val ASC";

    std::string input4 = "    select "
                         "       task.errcode as errcode2, "
                         "        task.errcategory as errcategory2,"
                         "        count() as errors_count2,"
                         "        toStartOfMinute(uts) as minute"
                         "    from dist_elog ARRAY JOIN tasks as task"
                         "    WHERE"
                         "        date = today() - count_day_before"
                         "        and uts > 'start_time' "
                         "        and uts < 'end_time'"
                         "        and (rtb_type = 1)"
                         "  and dynmon_version = 'unknown'"
                         "    GROUP BY"
                         "        errcode,"
                         "        errcategory,"
                         "        minute";

    std::string input5 = "Select kek, lol From (Select * From aue) Join lel on  aue.a = lel.b";
    std::string input6 = "select kek.kek2, time  from (select engine_id as kek2 from view_engine_event_horizon) as kek join view_engine_event_horizon as lol on kek.kek2 = lol.engine_id limit 10";
    std::string sql = input;
//    std::cin >> sql;
    ParserQueryWithOutput parser;
    ASTPtr ast = parseQuery(parser, sql.data(), sql.data() + sql.size(), "", 0, 0);

    std::cout << "Success." << std::endl;
    formatAST(*ast, std::cerr);
    std::cout << "\n\n" << std::endl;



//    std::shared_ptr<DB::ASTSelectQuery> X = std::dynamic_pointer_cast<DB::ASTSelectQuery>(ast);
//    if (X) {
//        std::cout << "SUCCESS";
//    }
//    print_indent(ast);
//    auto tablex = (*X).tables();
//    for (const auto & child : tablex)
//        std::cout << (*child).getColumnName() << "\n";

//    formatAST(*X, std::cerr);
//    std::cout << std::endl;
    TableList result;


    for (const auto & child: (*ast).children) {
        for (const auto & ch: (*child).children) {
            auto X = std::dynamic_pointer_cast<DB::ASTSelectQuery>(ch);
            if (X) {
                parse_sql(ch, result);
            }
        }
    }
    std::cout << "\n";
    result.print();

//    std::string x,y, i;
//    std::cin >> i;
//    std::tie(x, y) = get_table_a_column(i);
//    std::cout << x << "\n" << y << "\n";
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
    return 1;
}
