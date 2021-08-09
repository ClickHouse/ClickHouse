SELECT timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'));
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(10000));
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(10000), 600);
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(600), 30);
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), 'wrong argument'); -- { serverError 43 }
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(600), 'wrong argument'); -- { serverError 43 }
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(600), 0); -- { serverError 44 }