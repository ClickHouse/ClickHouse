function FAbs(value) {
    return value < 0 ? 0 - value : value;
}

function Trim(str) {
    return str.replace(/^\s+|\s+$/gm, '');
}

function LengthInUtf8Bytes(str) {
    // Matches only the 10.. bytes that are non-initial characters in a multi-byte sequence.
    var m = encodeURIComponent(str).match(/%[89ABab]/g);
    return str.length + (m ? m.length : 0);
}

function LengthInLineChars(terminal, char) {
    var $temp_terminal_prompt = terminal.find('.cmd-prompt').clone().css({visiblity: 'hidden', position: 'absolute'});

    $temp_terminal_prompt.appendTo(terminal.find('.cmd')).html(char);
    var bounding_client_rect = $temp_terminal_prompt[0].getBoundingClientRect();
    $temp_terminal_prompt.remove();
    return Math.floor(terminal.find('.terminal-fill').width() / bounding_client_rect.width);
}

var bytes_units = Array(" B", " KB", " MB", " GB", " TB", " PB", " EB", " ZB", " YB");
var rows_units = Array("", " thousand", " million", " billion", " trillion", " quadrillion");

function FormatReadable(value, units, delimiter)
{
    for (var index = 0; index + 1 < units.length; ++index)
    {
        if (FAbs(value) < delimiter || index + 1 >= units.length)
            return value.toFixed(2) + units[index];

        value /= delimiter;
    }
}

function FormatReadableQuantity(read_rows) {
    return FormatReadable(read_rows, rows_units, 1000.0)
}

function FormatReadableSizeWithDecimalSuffix(read_bytes) {
    return FormatReadable(read_bytes, bytes_units, 1024.0)
}

function ReservedCommandLine(input_line, terminal) {
    var reserved_exit_inputs = [
        "exit", "quit", "logout", "учше", "йгше", "дщпщге",
        "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж",
        "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"];

    if (reserved_exit_inputs.indexOf(input_line) !== -1)
    {
        terminal.clear();
        terminal.logout();
        return true;
    }

    return false;
}

function ProgressBar(terminal, width_of_progress_bar, read_rows, total_rows_corrected)
{
    var unicode_fill_ratio = LengthInLineChars(terminal, '█') / LengthInLineChars(terminal, '&nbsp;');

    read_rows = Math.min(Math.max(read_rows, 0), total_rows_corrected);
    width_of_progress_bar = Math.floor(width_of_progress_bar * unicode_fill_ratio);
    var unicode_fill_bar_width = read_rows / total_rows_corrected * width_of_progress_bar;

    var remainder = unicode_fill_bar_width - Math.floor(unicode_fill_bar_width);

    if (remainder)
    {
        remainder = Math.floor((unicode_fill_bar_width - Math.floor(unicode_fill_bar_width)) * 8);
        remainder = Array("▏", "▎", "▍", "▌", "▋", "▋", "▊", "▉")[remainder ? (remainder - 1) : 0];
    }

    return "\033[0;32m" + '█'.repeat(Math.floor(unicode_fill_bar_width)) + remainder + '\033[0m'
        + '\u3000'.repeat(Math.floor(width_of_progress_bar - unicode_fill_bar_width));
}

function ProgressBarAndStatus(terminal, progress, increment)
{
    var indicators = Array(
        "\033[1;30m→\033[0m", "\033[1;31m↘\033[0m", "\033[1;32m↓\033[0m",
        "\033[1;33m↙\033[0m", "\033[1;34m←\033[0m", "\033[1;35m↖\033[0m",
        "\033[1;36m↑\033[0m", "\033[1m↗\033[0m"
    );

    var progress_message = indicators[increment % 8] + " Progress: "
        + FormatReadableQuantity(progress.read_rows) + " rows, " + FormatReadableSizeWithDecimalSuffix(progress.read_bytes);

    if (!increment)
        progress_message += ". ";
    else
    {
        progress_message += (" (" + FormatReadableQuantity(progress.read_rows * 10.0 / increment) + " rows/s., "
            + FormatReadableSizeWithDecimalSuffix(progress.read_bytes * 10.0 / increment) + "/s.) ");
    }

    var written_progress_chars = LengthInUtf8Bytes(progress_message) - (increment % 8 === 7 ? 10 : 13);
    var width_chars_of_progress_bar = terminal.cols() - 2 /* progress bar */ - written_progress_chars - " 99%".length;

    /// If the approximate number of rows to process is known, we can display a progress bar and percentage.
    if (progress.total_rows_to_read > 0)
    {
        var total_rows_corrected = Math.max(progress.read_rows, progress.total_rows_to_read);

        /// To avoid flicker, display progress bar only if .5 seconds have passed since query execution start
        ///  and the query is less than halfway done.

        if (increment > 5)
        {
            /// Trigger to start displaying progress bar. If query is mostly done, don't display it.
            if (progress.read_rows * 2 < total_rows_corrected)
                progress.show_progress_bar = true;

            if (progress.show_progress_bar)
            {
                if (width_chars_of_progress_bar > 0)
                    progress_message += ProgressBar(terminal, width_chars_of_progress_bar, progress.read_rows, total_rows_corrected);
            }
        }

        /// Underestimate percentage a bit to avoid displaying 100%.
        progress_message += (' ' + (99 * progress.read_rows / total_rows_corrected).toFixed(0) + '%');
    }

    return progress_message;
}

function StartShowProgressBarAndStatus(terminal, timer_state)
{
    var progress = {
        "read_rows" : timer_state.progress.read_rows,
        "read_bytes": timer_state.progress.read_bytes,
        "total_rows_to_read" : timer_state.progress.total_rows_to_read
    };

    (function progress_bar_time_tick_loop() {
        if (!timer_state.shutdown_timer)
        {
            progress.read_rows = timer_state.progress.read_rows;
            progress.read_bytes = timer_state.progress.read_bytes;
            progress.total_rows_to_read = timer_state.progress.total_rows_to_read;

            terminal.set_prompt(ProgressBarAndStatus(terminal, progress, timer_state.increment++));
            timer_state.timer = setTimeout(progress_bar_time_tick_loop, 100);
        }
    })();
}

function ShowProgressBarAndStatus(terminal, state)
{
    var terminal_prompt = terminal.get_prompt();
    var timer_state = {"increment": 0, "shutdown_timer" : false, "progress" : state.progress};

    StartShowProgressBarAndStatus(terminal, timer_state);

    return function (prefix_echo) {
        timer_state.shutdown_timer = true;
        clearTimeout(timer_state.timer);

        var final_progress_message = prefix_echo + state.progress.rows_in_set + " rows in set. Elapsed: " + (timer_state.increment / 10).toFixed(2) + " sec. ";

        if (state.progress.read_rows >= 1000)
        {
            final_progress_message += "Processed " + FormatReadableQuantity(state.progress.read_rows) + " rows, "
                + FormatReadableSizeWithDecimalSuffix(state.progress.read_bytes);

            final_progress_message += !timer_state.increment ? ". " : (" (" + FormatReadableQuantity(state.progress.read_rows * 10.0 / timer_state.increment) + " rows/s., "
                + FormatReadableSizeWithDecimalSuffix(state.progress.read_bytes * 10.0 / timer_state.increment) + "/s.) ");
        }

        terminal.echo(final_progress_message).set_prompt(terminal_prompt);
    };
}

function StartReceiveDataPacket(terminal, running_query_id, state, callback_function) {
    (function receive_data_time_tick_loop() {
        jQuery.ajax({
            type: "POST",
            url: "output",
            contentType: "application/text;charset=utf-8",
            data: {running_query_id : running_query_id, authenticated_token : terminal.token()},
            error: function (xhr) { WebTerminalErrorHandle(xhr, terminal); callback_function(); },
            success: function (output_data, status, xhr) {
                var progress_info = xhr.getResponseHeader('X-ClickHouse-Progress');

                if (progress_info)
                {
                    progress_info = JSON.parse(progress_info);
                    state.progress.read_rows += parseInt(progress_info.read_rows);
                    state.progress.read_bytes += parseInt(progress_info.read_bytes);
                    state.progress.rows_in_set += parseInt(progress_info.rows_in_set);
                    state.progress.total_rows_to_read += parseInt(progress_info.total_rows_to_read);
                }

                if (xhr.status === 205)
                    callback_function();
                else
                {
                    if (output_data.length)
                        terminal.echo(output_data, {"newline": false});

                    setTimeout(receive_data_time_tick_loop, 0);
                }
            }
        });
    })();
}

function ReceiveExecutePacket(terminal, response_packet, state, callback_function) {
    switch (response_packet.type) {
        case 'started_query':
            terminal.echo(response_packet.echo_formatted_query);
            state.running_query_id = response_packet.running_query_id;
            var hide_progress_bar_handler = ShowProgressBarAndStatus(terminal, state);
            StartReceiveDataPacket(terminal, state.running_query_id, state, function () {
                hide_progress_bar_handler(state.cancel ? "Query was cancelled. \n\n" : "");
                callback_function();
            });
            break;
        default: terminal.error('Unknown packet from server'); callback(); break;
    }
}

function ReceiveKeyDown(session_state) {
    return function (event, terminal) {
        if (session_state.running_query_id)
        {
            if (event.which === 67 && event.ctrlKey)    // CTRL+C
            {
                session_state.cancel = true;

                jQuery.ajax({
                    url:"cancel",
                    type: "POST",
                    contentType: "application/text;charset=utf-8",
                    data: {running_query_id : session_state.running_query_id, authenticated_token : terminal.token()},
                    error: function (xhr) { WebTerminalErrorHandle(xhr, terminal); callback_function(); },
                    success: function (output_data, status, xhr) {
                        /// Do nothing, maybe output something/
                    }
                })
            }

            return false;
        }
    }
}

function ReceiveLoginPacket(user_name, user_password, callback_function) {
    var terminal = jQuery.terminal.active();
    jQuery.ajax({
        url: "login",
        type: "POST",
        contentType: "application/text;charset=utf-8",
        data: {user_name : user_name, user_password : user_password},
        error: function (xhr) { WebTerminalErrorHandle(xhr, terminal); callback_function(null); },
        success: function (response_data) {
            var response_packet = JSON.parse(response_data);
            switch (response_packet.type) {
                case 'login': terminal.clear(); callback_function(response_packet.authenticated_token); break;
                default: terminal.error('Unknown packet from server.'); callback_function(null); break;
            }
        }
    });
}

function WebTerminalErrorHandle(xhr, terminal) {
    if (xhr.status !== 500)
        return;

    var exception_msg = xhr.responseText;
    var exception_code = xhr.getResponseHeader('X-ClickHouse-Exception-Code');

    if (terminal === undefined)
        alert("Unable to initialize WebTerminal, Because " + exception_msg);

    if (exception_code != 192 && exception_code != 193 && exception_code != 194)
        terminal.echo(exception_msg);
    else
        terminal.clear().echo(terminal.token() ? "The session has expired. Please login again." : exception_msg).logout();
}

function WebTerminalInitHandle(configuration) {
    var session_state = {"running_query_id": "", "progress": {"read_rows": 0, "read_bytes": 0, "total_rows_to_read": 0, "rows_in_set": 0}};

    document.title = configuration.title;
    configuration.exit = false;
    configuration.login = ReceiveLoginPacket;
    configuration.keydown = ReceiveKeyDown(session_state);
    configuration.strings = {"wrongPasswordTryAgain" : ""};

    jQuery('body').terminal(function (input_line, terminal) {
        if (!ReservedCommandLine(input_line = Trim(input_line), terminal))
        {
            if (input_line.length !== 0)
            {
                terminal.pause();

                var has_vertical_output_suffix = '0';
                if (input_line.substring(input_line.length - 2) === '\\G')
                {
                    has_vertical_output_suffix = '1';
                    input_line = input_line.substring(0, input_line.length - 2);
                }

                jQuery.ajax({
                    type: "POST",
                    url: "execute_query",
                    contentType: "application/text;charset=utf-8",
                    data: {
                        execute_query: input_line,
                        authenticated_token: terminal.token(),
                        has_vertical_output_suffix: has_vertical_output_suffix
                    },
                    error: function (xhr) { WebTerminalErrorHandle(xhr, terminal); terminal.resume(); },
                    success: function (response_data) {
                        terminal.resume();
                        ReceiveExecutePacket(terminal, JSON.parse(response_data), session_state, function () {
                            session_state.cancel = false;
                            session_state.running_query_id = "";
                            session_state.progress.read_rows = 0;
                            session_state.progress.read_bytes = 0;
                            session_state.progress.rows_in_set = 0;
                            session_state.progress.total_rows_to_read = 0;
                        });
                    }
                });
            }
        }
    }, configuration);
}