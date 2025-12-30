select minMap(arrayJoin([([1], [null]), ([1], [null])])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select maxMap(arrayJoin([([1], [null]), ([1], [null])])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select sumMap(arrayJoin([([1], [null]), ([1], [null])])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select sumMapWithOverflow(arrayJoin([([1], [null]), ([1], [null])])); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

select minMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select maxMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMap(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
select sumMapWithOverflow(arrayJoin([([1, 2], [null, 11]), ([1, 2], [null, 22])]));
