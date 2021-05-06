Projects for various integrated development environments (IDE)
==============================================================

#### Included projects

The following projects are included with the lizard distribution:
- `VS2010` - Visual Studio 2010 project (which also works well with Visual Studio 2012, 2013, 2015)


#### How to compile lizard with Visual Studio

1. Install Visual Studio e.g. VS 2015 Community Edition (it's free).
2. Download the latest version of lizard from https://github.com/inikep/lizard/releases
3. Decompress ZIP archive.
4. Go to decompressed directory then to `visual` then `VS2010` and open `lizard.sln`
5. Visual Studio will ask about converting VS2010 project to VS2015 and you should agree.
6. Change `Debug` to `Release` and if you have 64-bit Windows change also `Win32` to `x64`.
7. Press F7 on keyboard or select `BUILD` from the menu bar and choose `Build Solution`.
8. If compilation will be fine a compiled executable will be in `visual\VS2010\bin\x64_Release\lizard.exe`


#### Projects available within lizard.sln

The Visual Studio solution file `lizard.sln` contains many projects that will be compiled to the
`visual\VS2010\bin\$(Platform)_$(Configuration)` directory. For example `lizard` set to `x64` and
`Release` will be compiled to `visual\VS2010\bin\x64_Release\lizard.exe`. The solution file contains the
following projects:

- `lizard` : Command Line Utility, supporting gzip-like arguments
- `datagen` : Synthetic and parametrable data generator, for tests
- `frametest` : Test tool that checks lizard_frame integrity on target platform
- `fullbench`  : Precisely measure speed for each lizard inner functions
- `fuzzer` : Test tool, to check lizard integrity on target platform 
- `liblizard` : A static Lizard library compiled to `liblizard_static.lib`
- `liblizard-dll` : A dynamic Lizard library (DLL) compiled to `liblizard.dll` with the import library `liblizard.lib`
- `fullbench-dll` : The fullbench program compiled with the import library; the executable requires Lizard DLL


#### Using Lizard DLL with Microsoft Visual C++ project

The header files `lib\lizard.h`, `lib\lizardhc.h`, `lib\lizard_frame.h` and the import library
`visual\VS2010\bin\$(Platform)_$(Configuration)\liblizard.lib` are required to compile a
project using Visual C++.

1. The path to header files should be added to `Additional Include Directories` that can
   be found in Project Properties of Visual Studio IDE in the `C/C++` Property Pages on the `General` page.
2. The import library has to be added to `Additional Dependencies` that can
   be found in Project Properties in the `Linker` Property Pages on the `Input` page.
   If one will provide only the name `liblizard.lib` without a full path to the library
   then the directory has to be added to `Linker\General\Additional Library Directories`.

The compiled executable will require Lizard DLL which is available at
`visual\VS2010\bin\$(Platform)_$(Configuration)\liblizard.dll`.
