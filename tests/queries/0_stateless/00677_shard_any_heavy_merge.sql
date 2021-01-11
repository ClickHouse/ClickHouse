WITH arrayJoin(['hello', 'world']) AS s SELECT count(), arraySort(groupUniqArray(s)), anyHeavy(s) FROM remote('127.0.0.{2,3}', system.one);
