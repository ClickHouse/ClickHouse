SELECT endsWith(s, 'ow') FROM (SELECT arrayJoin(['', 'o', 'ow', 'Hellow', '3434', 'owfffffffdHe']) AS s);
SELECT endsWith(s, '') FROM (SELECT arrayJoin(['', 'h', 'hi']) AS s);
