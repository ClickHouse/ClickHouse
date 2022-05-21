select generateUUIDv4() from numbers(10);

select generateUUIDv4(1) from numbers(10);

select generateUUIDv4(2) from numbers(10);

select generateUUIDv4(1), generateUUIDv4(1) from numbers(10);

select generateUUIDv4(), generateUUIDv4() from numbers(10);

select generateUUIDv4(1), generateUUIDv4() from numbers(10);

select generateUUIDv4(), generateUUIDv4(1) from numbers(10);

select generateUUIDv4(1), generateUUIDv4(2) from numbers(10);

select generateUUIDv4(1), generateUUIDv4(1), generateUUIDv4(1) from numbers(10);

select generateUUIDv4(1), generateUUIDv4(2), generateUUIDv4(3) from numbers(10);
