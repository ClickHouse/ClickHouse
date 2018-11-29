SELECT
    firstSignificantSubdomain('http://hello.canada.ca') AS canada,
    firstSignificantSubdomain('http://hello.congo.com') AS congo,
    firstSignificantSubdomain('http://pochemu.net-domena.ru') AS why;

SELECT
    firstSignificantSubdomain('ftp://www.yandex.com.tr/news.html'),
    firstSignificantSubdomain('https://www.yandex.ua/news.html'),
    firstSignificantSubdomain('magnet:yandex.abc'),
    firstSignificantSubdomain('ftp://www.yandex.co.uk/news.html'),
    firstSignificantSubdomain('ftp://yandex.co.yandex'),
    firstSignificantSubdomain('http://ввв.яндекс.org.рф'),
    firstSignificantSubdomain('https://api.www3.static.dev.ввв.яндекс.рф'),
    firstSignificantSubdomain('//www.yandex.com.tr/news.html');

SELECT
    firstSignificantSubdomain('http://hello.canada.c'),
    firstSignificantSubdomain('http://hello.canada.'),
    firstSignificantSubdomain('http://hello.canada'),
    firstSignificantSubdomain('http://hello.c'),
    firstSignificantSubdomain('http://hello.'),
    firstSignificantSubdomain('http://hello'),
    firstSignificantSubdomain('http://'),
    firstSignificantSubdomain('http:/'),
    firstSignificantSubdomain('http:'),
    firstSignificantSubdomain('http'),
    firstSignificantSubdomain('h'),
    firstSignificantSubdomain('.'),
    firstSignificantSubdomain(''),
    firstSignificantSubdomain('http://hello.canada..com'),
    firstSignificantSubdomain('http://hello..canada.com'),
    firstSignificantSubdomain('http://hello.canada.com.');
