select splitByRegexp('\\d+', 'a1ba5ba8b');
select splitByRegexp('', 'abcde');
select splitByRegexp('<[^<>]*>', x) from (select arrayJoin(['<h1>hello<h2>world</h2></h1>', 'gbye<split>bug']) x);
select splitByRegexp('ab', '');
select splitByRegexp('', '');