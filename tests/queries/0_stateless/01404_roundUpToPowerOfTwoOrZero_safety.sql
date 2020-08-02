SELECT repeat('0.0001048576', number * (number * (number * 255))) FROM numbers(65535); -- { serverError 241; }
