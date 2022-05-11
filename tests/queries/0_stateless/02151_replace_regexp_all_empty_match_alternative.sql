select replaceRegexpAll(',,1,,', '^[,]*|[,]*$', '') x;
select replaceRegexpAll(',,1', '^[,]*|[,]*$', '') x;
select replaceRegexpAll('1,,', '^[,]*|[,]*$', '') x;
