#pragma once

#if !defined(USE_MUSL)
void checkHarmfulEnvironmentVariables(char ** argv);
#endif
