SELECT initCap(arrayJoin(['hello', 'world'])::FixedString(5));
SELECT initCap(arrayJoin(['hello world', 'world hello'])::FixedString(11));
