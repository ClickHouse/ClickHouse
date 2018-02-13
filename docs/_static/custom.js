$(function() {
    $('a[href="#edit"]').on('click', function(e) {
        e.preventDefault();
        var pathname = window.location.pathname;
        var url;
        if (pathname.indexOf('html') >= 0) {
            url = pathname.replace('/docs/', 'https://github.com/yandex/ClickHouse/edit/master/docs/').replace('html', 'md');
        } else {
            if (pathname.indexOf('/single/') >= 0) {
                if (pathname.indexOf('ru') >= 0) {
                    url = 'https://github.com/yandex/ClickHouse/tree/master/docs/ru';
                } else {
                    url = 'https://github.com/yandex/ClickHouse/tree/master/docs/en';
                }
            } else {
                if (pathname.indexOf('ru') >= 0) {
                    url = 'https://github.com/yandex/ClickHouse/edit/master/docs/ru/index.md';
                } else {
                    url = 'https://github.com/yandex/ClickHouse/edit/master/docs/en/index.md';
                }
            }
        }
        if (url) {
            var win = window.open(url, '_blank');
            win.focus();
        }
    });

    $('a[href="#en"]').on('click', function(e) {
        e.preventDefault();
        window.location = window.location.toString().replace('/ru/', '/en/');
    });

    $('a[href="#ru"]').on('click', function(e) {
        e.preventDefault();
        window.location = window.location.toString().replace('/en/', '/ru/');
    });
});
