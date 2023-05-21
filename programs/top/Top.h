#pragma once

#include <unordered_map>
#include <ncurses.h>
#include "../client/Client.h"
#include "Interpreters/Context.h"

namespace DB
{

class Top : public Client
{
public:
    void start();

    void initNcurses();

    void parseMetric(String & str);

    void printLine(int ind, std::vector<int> & indents, bool is_header);

    void printProcessTable();

    void printTop();

    void printHelpBar();

    void showLineDescription();

    void setMemorySortedQuery();

    bool tryKeyboard();

    int sleepTryKeyboard();

    String queryToString(String& query);

    int makeProcessTable();

    int makeTop();

    void parseTopQuery(String& str);

    void go();

private:
    WINDOW * top_win;
    std::unordered_map<String, String> top_data;

    WINDOW * table_win;
    std::vector<std::vector<String>> process_table;
    int table_start_row;
    int table_end_row;
    int highlight;

    WINDOW * bottom_win;

    String process_query;
};
}
