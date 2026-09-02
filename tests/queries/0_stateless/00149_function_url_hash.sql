select URLHash('' as url) = URLHash(appendTrailingCharIfAbsent(url, '/'));
select URLHash('http://ya.ru' as url) = URLHash(appendTrailingCharIfAbsent(url, '/'));
select URLHash('http://ya.ru' as url) = URLHash(appendTrailingCharIfAbsent(url, '?'));
select URLHash('http://ya.ru' as url) = URLHash(appendTrailingCharIfAbsent(url, '#'));

select URLHash('' as url, 0) = URLHash(url);
select URLHash('' as url, 1) = URLHash(url);
select URLHash('' as url, 1000) = URLHash(url);

select URLHash('http://ya.ru/a' as url, 0 as level) = URLHash(URLHierarchy(url)[level + 1]);
select URLHash('http://ya.ru/a' as url, 1 as level) = URLHash(URLHierarchy(url)[level + 1]);

select URLHash(url, 0 as level) = URLHash(URLHierarchy(url)[level + 1]) from system.one array join ['', 'http://ya.ru', 'http://ya.ru/', 'http://ya.ru/a', 'http://ya.ru/a/', 'http://ya.ru/a/b', 'http://ya.ru/a/b?'] as url;
select URLHash(url, 1 as level) = URLHash(URLHierarchy(url)[level + 1]) from system.one array join ['', 'http://ya.ru', 'http://ya.ru/', 'http://ya.ru/a', 'http://ya.ru/a/', 'http://ya.ru/a/b', 'http://ya.ru/a/b?'] as url;
select URLHash(url, 2 as level) = URLHash(URLHierarchy(url)[level + 1]) from system.one array join ['', 'http://ya.ru', 'http://ya.ru/', 'http://ya.ru/a', 'http://ya.ru/a/', 'http://ya.ru/a/b', 'http://ya.ru/a/b?'] as url;
select URLHash(url, 3 as level) = URLHash(URLHierarchy(url)[level + 1]) from system.one array join ['', 'http://ya.ru', 'http://ya.ru/', 'http://ya.ru/a', 'http://ya.ru/a/', 'http://ya.ru/a/b', 'http://ya.ru/a/b?'] as url;
select URLHash(url, 4 as level) = URLHash(URLHierarchy(url)[level + 1]) from system.one array join ['', 'http://ya.ru', 'http://ya.ru/', 'http://ya.ru/a', 'http://ya.ru/a/', 'http://ya.ru/a/b', 'http://ya.ru/a/b?'] as url;
