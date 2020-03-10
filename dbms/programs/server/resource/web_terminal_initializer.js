jQuery(document).ready(function($) {
    jQuery.ajax({
        type: "POST",
        url: "configuration",
        contentType: "application/json;charset=utf-8",
        error: function (xhr) { WebTerminalErrorHandle(xhr, undefined); },
        success: function (configuration) {WebTerminalInitHandle(JSON.parse(configuration));}
    });
});
