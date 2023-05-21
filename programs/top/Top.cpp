#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/UseSSL.h>
#include "../client/Client.h"

#include <sys/signal.h>

#include <csignal>
#include "AggregateFunctions/IAggregateFunction.h"
#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "Top.h"
#include "config.h"

#include <chrono>
#include <string>
#include <thread>

#include <cmath>
#include <format>


#ifndef __clang__
#    pragma GCC optimize("-fno-var-tracking-assignments")
#endif


#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

namespace DB
{

//------------COLORS-------------//


namespace C // color_pairs
{
    enum
    {
        BLACK_GREEN = 1,
        BLACK_CYAN = 2
    };
}

void init_colors()
{
    init_pair(C::BLACK_GREEN, COLOR_BLACK, COLOR_GREEN);
    init_pair(C::BLACK_CYAN, COLOR_BLACK, COLOR_CYAN);
}


//-------------------------------//

//-----------NCURSES-------------//

bool is_ncurses_mode = false;

const int TABLE_START_Y = 10;

[[noreturn]] void handler_sigint(int signal)
{
    (void)signal;
    if (is_ncurses_mode)
    {
        endwin();
        is_ncurses_mode = false;
    }
    _exit(SIGINT);
}


void Top::initNcurses()
{
    struct sigaction act = {};
    act.sa_handler = handler_sigint;

    initscr();
    sigaction(SIGINT, &act, nullptr); // must be after initscr() according to curses documentation
    noecho(); // user doesn't see what he presses
    cbreak(); // disable line buffering
    start_color();
    init_colors();
    curs_set(0);
    is_ncurses_mode = true;

    int startx = 0;
    int starty = 0;
    int width = COLS;
    int height = TABLE_START_Y;
    top_win = newwin(height, width, starty, startx);

    startx = 0;
    starty = TABLE_START_Y;
    width = COLS;
    height = LINES - 1 - starty;
    table_win = newwin(height, width, starty, startx);
    table_start_row = 1; // not counting header
    table_end_row = LINES - 1;

    starty = LINES - 1;
    height = 1;
    bottom_win = newwin(height, width, starty, startx);
    keypad(bottom_win, TRUE);
    nodelay(bottom_win, TRUE); // wgetch becomes non-blocking, needed for arrow keys navigation

    highlight = 1;
}

void resetWin(WINDOW * win)
{
    wclear(win);
    wmove(win, 0, 0);
}

//-------------------------------//

namespace BYTE
{
    float MB = 1024 * 1024;
    float GB = MB * 1024; // in bytes
    float TB = GB * 1024;
}


namespace Q
{ // Queries
    enum
    {
        METRIC = 0,
        ASYNC_METRIC = 1,
        EVENT = 2,
        PROCESS = 3
    };

    std::vector<String> h_metric = {"metric", "value", "description"};
    std::vector<String> h_async_metric = h_metric;
    std::vector<String> h_async_event = {"event", "value", "description"};


    std::vector<String> h_process = {"r_progr", "read", "wrote", "RAM", "RAM%%", "time", "kill", "usr", "query"};


    std::vector<std::vector<String>> headers = {h_metric, h_async_metric, h_async_event, h_process};

    String epilogue = " INTO OUTFILE 'top/out.txt' FORMAT TabSeparated";

    String q_metrics = "SELECT * FROM system.metrics" + epilogue;
    String q_async_metrics = "SELECT * FROM system.asynchronous_metrics" + epilogue;
    String q_events = "SELECT * FROM system.events" + epilogue;
    String q_processes_base = "SELECT read_rows / total_rows_approx, read_bytes, written_bytes, memory_usage, ROUND(memory_usage / (SELECT "
                              "SUM(memory_usage) FROM system.processes) * 100, 1) "
                              ", ROUND(elapsed, 1), is_cancelled, user, query FROM system.processes ";
    String q_processes = q_processes_base + epilogue;

    String q_processes_ram_sort_inc = q_processes_base + " ORDER BY memory_usage" + epilogue;
    String q_processes_ram_sort_dec = q_processes_base + "ORDER BY memory_usage DESC" + epilogue;
    int ram_sort = 0;

    std::vector<String> queries = {q_metrics, q_async_metrics, q_events, q_processes};


    std::vector<String> h_top = {"Query", "TCPConnection", "HTTPConnection", "MaxPartCountForPartition", "TotalPartsOfMergeTreeTables", "TotalRowsOfMergeTreeTables", "ReplicasMaxQueueSize"};

    String q_top_metrics
        = "SELECT metric, value FROM system.metrics WHERE metric == 'Query' OR metric == 'TCPConnection' OR metric == 'HTTPConnection'"
        + epilogue;
    String q_top_async_metrics
        = "SELECT metric, value FROM system.asynchronous_metrics WHERE metric == 'MaxPartCountForPartition' OR "
          "metric == 'TotalPartsOfMergeTreeTables' OR metric == 'TotalRowsOfMergeTreeTables' OR metric == ' ReplicasMaxQueueSize'"
        + epilogue;
    // ADD SERVER UPTIME

    String help_bar_str = "F1 Help";
}

namespace P
{ // progress constants
    enum
    {
        READ_PROGRESS = 0,
        READ_BYTES = 1,
        WROTE_BYTES = 2,
        MEMORY_USAGE = 3,
        ELAPSED = 5
    };
}

//--------------GENERAL--------------------//

void Top::printHelpBar()
{
    wmove(bottom_win, 0, 0);
    wattron(bottom_win, COLOR_PAIR(C::BLACK_CYAN));
    wprintw(bottom_win, Q::help_bar_str.data());
    wattroff(bottom_win, COLOR_PAIR(C::BLACK_CYAN));
    wrefresh(bottom_win);
}

void showHelpScreen() // can be done in different window and just refreshing
{
    resetWin(stdscr);

    wprintw(stdscr, "We are trying to help.\n Press any key to return.");

    wrefresh(stdscr);

    wgetch(stdscr);
}

bool Top::tryKeyboard()
{
    int ch = wgetch(bottom_win);
    switch (ch)
    {
        case '\n':
            showLineDescription();
            return true;
        case 'm':
            setMemorySortedQuery();
            return true;
        case KEY_F(1):
            showHelpScreen();
            return true;
        case KEY_UP:
            --highlight;
            return true;
        case KEY_DOWN:
            ++highlight;
            return true;
        default:
            return false;
    }
}

int Top::sleepTryKeyboard()
{
    for (int i = 0; i < 1; ++i)
    {
        if (tryKeyboard())
        {
            return 1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return 0;
}

//-------------------------------//

//-----------PROGRESS TABLE-------------//

void add_progressbar(std::vector<std::vector<String>> & a)
{
    int length = static_cast<int>(COLS * 0.2);
    for (size_t i = 1; i < a.size(); ++i)
    {
        auto & vec = a[i];
        double progress = std::stod(vec[P::READ_PROGRESS]);
        if (std::isinf(progress) || std::isnan(progress)) // very necessary part to check.
        {
            progress = 0;
            vec[P::READ_PROGRESS] = "NO";
            return;
        }


        int j = 1;
        while (static_cast<double>(j) / length < progress)
        {
            ++j;
        }
        vec[P::READ_PROGRESS] = String(j, '|') + String(length - j, ' ');
    }
}

void parseBytes(String & data)
{
    float bytes = std::stof(data);
    float value;
    String res;
    if (bytes < BYTE::GB)
    { // always round up
        value = bytes / BYTE::MB;
        res = std::format("{:.0f}", value) + "MB";
    }
    else if (bytes < BYTE::TB)
    {
        value = bytes / BYTE::GB;
        res = std::format("{:.2f}", value) + "GB";
    }
    else
    {
        value = bytes / BYTE::TB;
        res = std::format("{:.2f}", value) + "TB";
    }
    data = res;
}

void reformat_process_table(std::vector<std::vector<String>> & a)
{
    add_progressbar(a);
    for (size_t i = 1; i < a.size(); ++i)
    {
        auto & vec = a[i];
        parseBytes(vec[P::READ_BYTES]);
        parseBytes(vec[P::WROTE_BYTES]);
        parseBytes(vec[P::MEMORY_USAGE]);
    }
}


void Top::parseMetric(String & str)
{
    process_table.clear();

    process_table.push_back(Q::headers[Q::PROCESS]);

    std::vector<String> cur_vec;
    String cur_str;
    for (auto sym : str)
    {
        if (sym == '\t' || sym == '\n')
        {
            if (!cur_str.empty())
            {
                cur_vec.push_back(cur_str);
                cur_str.clear();
            }
            if (sym == '\n')
            {
                process_table.push_back(cur_vec);
                cur_vec.clear();
            }
        }
        else
        {
            cur_str.push_back(sym);
        }
    }
    if (!cur_str.empty())
    {
        cur_vec.push_back(cur_str);
        process_table.push_back(cur_vec);
    }

    reformat_process_table(process_table);
}

std::vector<int> get_indents(std::vector<std::vector<String>> & a)
{
    if (a.empty())
    {
        perror("Passed Vector Is Empty");
        _exit(1);
    }
    std::vector<int> indents(a[0].size());

    for (auto & vec : a)
    {
        for (size_t i = 0; i < vec.size(); ++i)
        {
            indents[i] = std::max(indents[i], static_cast<int>(vec[i].size()));
        }
    }

    return indents;
}

void Top::printLine(int ind, std::vector<int> & indents, bool is_header)
{
    std::vector<String> vec = process_table[ind];
    for (size_t j = 0; j < vec.size(); ++j)
    {
        String str = vec[j].substr(0, COLS - getcurx(table_win) - 1);

        if (!is_header && j == 0)
        {
            if (highlight == ind)
            {
                wattroff(table_win, COLOR_PAIR(C::BLACK_GREEN));
            }
            wattron(table_win, COLOR_PAIR(C::BLACK_CYAN));
            wprintw(table_win, str.data());
            wattroff(table_win, COLOR_PAIR(C::BLACK_CYAN));
            if (highlight == ind)
            {
                wattron(table_win, COLOR_PAIR(C::BLACK_GREEN));
            }
        }
        else
        {
            wprintw(table_win, str.data());
        }

        int xdif = (indents[j] + 1 - static_cast<int>(str.size())); // + 1 for a delimeter
        if (j != vec.size() - 1)
        {
            wprintw(table_win, String(xdif, ' ').data());
        }
        else
        {
            wprintw(table_win, String(COLS - getcurx(table_win) - 1, ' ').data());
        }
    }
    wmove(table_win, getcury(table_win) + 1, 0);
}


void Top::printProcessTable()
{
    resetWin(table_win);
    auto indents = get_indents(process_table);

    wattron(table_win, COLOR_PAIR(C::BLACK_GREEN)); // printing header
    printLine(0, indents, true);
    wattroff(table_win, COLOR_PAIR(C::BLACK_GREEN));

    if (highlight < 1)
    {
        highlight = 1;
    }
    int cur_size = static_cast<int>(process_table.size());
    if (highlight >= cur_size) // cur_size is always >= 2
    {
        highlight = cur_size - 1;
    }

    table_end_row = table_start_row + LINES - 2; // -2 because we are allowed Lines - 2 space (bcof header)
    if (highlight >= table_end_row)
    { // scrolling down
        ++table_end_row;
        ++table_start_row;
    }
    if (highlight < table_start_row)
    { // scrolling up
        --table_end_row;
        --table_start_row;
    }
    table_end_row = std::min(table_end_row, cur_size);


    for (int i = table_start_row; i < table_end_row; ++i) // printing data
    {
        if (i == highlight)
        {
            wattron(table_win, COLOR_PAIR(C::BLACK_GREEN)); // printing header
            printLine(i, indents, false);
            wattroff(table_win, COLOR_PAIR(C::BLACK_GREEN));
        }
        else
        {
            printLine(i, indents, false);
        }
    }
    wrefresh(table_win);
}

void Top::showLineDescription()
{
    resetWin(stdscr);

    std::vector<String> cur_header = Q::headers[Q::PROCESS];

    for (size_t i = 0; i < cur_header.size(); ++i)
    {
        String name = cur_header[i];
        String value = process_table[highlight][i];

        wattron(stdscr, COLOR_PAIR(C::BLACK_GREEN));
        wprintw(stdscr, "%s", name.data());
        wattroff(stdscr, COLOR_PAIR(C::BLACK_GREEN));

        wprintw(stdscr, ": %s\n", value.data());
    }
    wrefresh(stdscr);

    wgetch(stdscr); // press any key to return
}


void Top::setMemorySortedQuery()
{
    if (Q::ram_sort != 1)
    {
        this->process_query = Q::q_processes_ram_sort_dec;
        Q::ram_sort = 1;
    }
    else
    {
        this->process_query = Q::q_processes_ram_sort_inc;
        Q::ram_sort = -1;
    }
}

int Top::makeProcessTable()
{
    String str = queryToString(this->process_query);
    if (sleepTryKeyboard())
    {
        return 0;
    }

    parseMetric(str);
    if (sleepTryKeyboard())
    {
        return 0;
    }

    printProcessTable();
    if (sleepTryKeyboard())
    {
        return 0;
    }
    return 1;
}

//-------------------------------//

//-------------TOP--METRICS------------------//

void Top::printTop()
{
    resetWin(top_win);

    for (auto& metric : Q::h_top) {
        wprintw(top_win, "%s: %s\n", metric.data(), top_data[metric].data());
    }

    wrefresh(top_win);
}

void Top::parseTopQuery(String & str)
{ // processes only "metric, value". could be extended to description
    String cur_str;
    String cur_metric;
    for (auto sym : str)
    {
        if (sym == '\t' || sym == '\n')
        {
            if (cur_metric.empty())
            {
                cur_metric = cur_str;
            }
            else
            {
                top_data[cur_metric] = cur_str;
                cur_metric.clear();
            }
            cur_str.clear();
        }
        else
        {
            cur_str.push_back(sym);
        }
    }
    if (!cur_str.empty())
    {
        top_data[cur_metric] = cur_str;
    }
}



String Top::queryToString(String & query)
{
    remove("top/out.txt");
    processQueryText(query);
    String str;
    char c;
    ReadBufferFromFile read_buffer("top/out.txt");
    while (true)
    {
        int status = read_buffer.read(c);
        if (status == read_buffer.eof())
        {
            break;
        }
        str.push_back(c);
    }
    read_buffer.close();
    return str;
}

int Top::makeTop()
{
    String str = queryToString(Q::q_top_metrics) + "\n" + queryToString(Q::q_top_async_metrics);

    if (sleepTryKeyboard())
    {
        return 0;
    }
    parseTopQuery(str);

    if (sleepTryKeyboard())
    {
        return 0;
    }

    printTop();
    if (sleepTryKeyboard())
    {
        return 0;
    }

    return 1;
}

//---------------------------------------//

//---------------RUNNING-------------------//

void Top::go()
{
    initNcurses();
    printHelpBar();
    this->process_query = Q::queries[Q::PROCESS];

    int cnt = 0;

    while (true)
    {
        if (!makeProcessTable())
        {
            continue;
        }

        if (!makeTop())
        {
            continue;
        }


        if (cnt == 50000)
        {
            break;
        }
        ++cnt;
    }

    endwin();
    is_ncurses_mode = false;
}


void Top::start()
{
    // part of rewritten client function
    initialize(*this);

    UseSSL use_ssl;
    MainThreadStatus::getInstance();
    setupSignalHandler();

    std::cout << std::fixed << std::setprecision(3);
    std::cerr << std::fixed << std::setprecision(3);

    registerFormats();
    registerFunctions();
    registerAggregateFunctions();

    processConfig();

    connect();

    connection->setDefaultDatabase(connection_parameters.default_database);

    // my code
    is_interactive = false;
    go();
}
}

int mainEntryClickHouseTop(int argc, char ** argv)
{
    DB::Top client;
    client.init(argc, argv);
    client.start();
    return 0;
}
