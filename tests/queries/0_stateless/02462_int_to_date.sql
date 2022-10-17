select toYYYYMMDD(toDate(recordTimestamp, 'Europe/Amsterdam')), toDate(recordTimestamp, 'Europe/Amsterdam'), toInt64(1665519765) as recordTimestamp;
select toYYYYMMDD(toDate(recordTimestamp, 'Europe/Amsterdam')), toDate(recordTimestamp, 'Europe/Amsterdam'), toUInt64(1665519765) as recordTimestamp;
select toYYYYMMDD(toDate(recordTimestamp, 'Europe/Amsterdam')), toDate(recordTimestamp, 'Europe/Amsterdam'), toInt32(1665519765) as recordTimestamp, toTypeName(recordTimestamp);
select toYYYYMMDD(toDate(recordTimestamp, 'Europe/Amsterdam')), toDate(recordTimestamp, 'Europe/Amsterdam'), toUInt32(1665519765) as recordTimestamp, toTypeName(recordTimestamp);
