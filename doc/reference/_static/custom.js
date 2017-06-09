$(function() {
    $('a[href="#edit"]').on('click', function(e) {
        e.preventDefault();
        var pathname = window.location.pathname;
        var url;
        if (pathname.indexOf('html') >= 0) {
            url = pathname.replace('/docs/', 'https://github.com/yandex/ClickHouse/edit/master/doc/reference/').replace('html', 'rst');
        } else {
            if (pathname.indexOf('/single/') >= 0) {
                if (pathname.indexOf('ru') >= 0) {
                    url = 'https://github.com/yandex/ClickHouse/tree/master/doc/reference/ru';
                } else {
                    url = 'https://github.com/yandex/ClickHouse/tree/master/doc/reference/en';
                }
            } else {
                if (pathname.indexOf('ru') >= 0) {
                    url = 'https://github.com/yandex/ClickHouse/edit/master/doc/reference/ru/index.rst';
                } else {
                    url = 'https://github.com/yandex/ClickHouse/edit/master/doc/reference/en/index.rst';
                }
            }
        }
        if (url) {
            var win = window.open(url, '_blank');
            win.focus();
        }
    });
});
