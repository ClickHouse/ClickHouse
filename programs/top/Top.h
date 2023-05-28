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

    void initNcurses();

    void parseMetric(String & str);

    void printLine(int ind, std::vector<int> & indents, bool is_header);

    void printProcessTable();

    void printTop();

    void printHelpBar();

    void printLineDescription();

    void setSortedQuery(char option);

    void reformatProcessTable();

    void showHelpScreen();


    bool tryKeyboard();

    int sleepTryKeyboard();

    String queryToString(String& query);

    void addProgressbar();

    

    void parseTopQuery(String& str);

    int makeProcessTable();

    int makeTop();

    void go();

    void start();

private:
    WINDOW * top_win;
    std::unordered_map<String, String> top_data;
    int top_y;

    WINDOW * table_win;
    std::vector<std::vector<String>> process_table;
    int table_start_row;
    int table_end_row;
    int highlight;

    WINDOW * bottom_win;

    String process_query;
};
}
