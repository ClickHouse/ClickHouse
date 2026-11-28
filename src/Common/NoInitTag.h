#pragma once

/// Selects a constructor that leaves the contents uninitialized, for objects that are filled right away.
struct NoInitTag
{
};
