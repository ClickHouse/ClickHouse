SELECT any(nullIf(s, '')) FROM (SELECT arrayJoin(['', 'Hello']) AS s);
