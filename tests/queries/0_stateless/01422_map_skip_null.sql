select minMap(arrayJoin([([1], [null]), ([1], [null])]));
select maxMap(arrayJoin([([1], [null]), ([1], [null])]));
select sumMap(arrayJoin([([1], [null]), ([1], [null])])); -- { serverError 43 }
select sumMapWithOverflow(arrayJoin([([1], [null]), ([1], [null])]));

select minMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select maxMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMapWithOverflow(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
