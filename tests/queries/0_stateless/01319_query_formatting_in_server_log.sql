SeLeCt 'ab
cd' /* hello */ -- world
, 1;

SYSTEM FLUSH LOGS;
SELECT extract(message, 'SeL.+?;') FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%SeLeCt \'ab\n%' ORDER BY event_time DESC LIMIT 1 FORMAT TSVRaw;
