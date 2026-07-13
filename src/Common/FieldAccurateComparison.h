#pragma once

namespace DB
{
class Field;

bool accurateEquals(const Field & left, const Field & right);
bool accurateLess(const Field & left, const Field & right);
bool accurateLessOrEqual(const Field & left, const Field & right);

}
