#pragma once

#if defined(WYRD_SIMULATION)
#    define WYRD_NAMESPACE(name) name::inline UnderSimulation
#elif defined(WYRD_ADVERSARY)
#    define WYRD_NAMESPACE(name) name::inline UnderAdversary
#else
#    define WYRD_NAMESPACE(name) name
#endif
