-- length should be 32
select length(hex(generateUUIDv4()));

with generateUUIDv4() as uuid,
    hex(reverse(reinterpretAsString(uuid))) as str1,
    hex(uuid) as str2
select str1 = str2;

-- hex on UUID always generate 32 characters even there're leading zeros
select lower(hex(toUUID('00000000-80e7-46f8-0000-9d773a2fd319')));

with generateUUIDv4() as uuid,
    bin(reverse(reinterpretAsString(uuid))) as bin_str1,
    bin(uuid) as bin_str2
select bin_str1 = bin_str2;

-- bin on UUID always generate 128 characters even there're leading zeros
select length(bin(toUUID('00000000-80e7-46f8-0000-9d773a2fd319')));