---
description: 'Guía para integrar bibliotecas de Rust en ClickHouse'
sidebar_label: 'Bibliotecas de Rust'
slug: /development/integrating_rust_libraries
title: 'Integrar bibliotecas de Rust'
doc_type: 'guide'
---

La integración de bibliotecas de Rust se describirá tomando como ejemplo la integración de la función hash BLAKE3.

El primer paso de la integración es añadir la biblioteca a la carpeta /rust. Para ello, debes crear un proyecto de Rust vacío e incluir la biblioteca necesaria en Cargo.toml. También es necesario configurar la compilación de la nueva biblioteca como estática añadiendo `crate-type = ["staticlib"]` a Cargo.toml.

A continuación, debes enlazar la biblioteca con CMake mediante la biblioteca Corrosion. El primer paso es añadir la carpeta de la biblioteca al CMakeLists.txt dentro de la carpeta /rust. Después, debes añadir el archivo CMakeLists.txt al directorio de la biblioteca. En él, debes llamar a la función de importación de Corrosion. Estas líneas se utilizaron para importar BLAKE3:

```CMake
corrosion_import_crate(MANIFEST_PATH Cargo.toml NO_STD)

target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
```

Así, crearemos un destino de CMake correcto con Corrosion y luego lo renombraremos con un nombre más práctico. Tenga en cuenta que el nombre `_ch_rust_blake3` proviene de Cargo.toml, donde se usa como nombre del proyecto (`name = "_ch_rust_blake3"`).

Como los tipos de datos de Rust no son compatibles con los de C/C++, usaremos nuestro proyecto de biblioteca vacío para crear shim methods que conviertan los datos recibidos desde C/C++, llamen a los methods de la biblioteca y realicen la conversión inversa de los datos de salida. Por ejemplo, este method se escribió para BLAKE3:

```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
```

```rust
#[no_mangle]
pub unsafe extern "C" fn blake3_apply_shim(
    begin: *const c_char,
    _size: u32,
    out_char_data: *mut u8,
) -> *mut c_char {
    if begin.is_null() {
        let err_str = CString::new("input was a null pointer").unwrap();
        return err_str.into_raw();
    }
    let mut hasher = blake3::Hasher::new();
    let input_bytes = CStr::from_ptr(begin);
    let input_res = input_bytes.to_bytes();
    hasher.update(input_res);
    let mut reader = hasher.finalize_xof();
    reader.fill(std::slice::from_raw_parts_mut(out_char_data, blake3::OUT_LEN));
    std::ptr::null_mut()
}
```

Este método toma como entrada una cadena compatible con C, su tamaño y un puntero a la cadena de salida. Luego, convierte esas entradas compatibles con C en los tipos que usan los métodos reales de la biblioteca y los invoca. Después, debe convertir las salidas de los métodos de la biblioteca de nuevo a un tipo compatible con C. En este caso concreto, la biblioteca permitía escribir directamente en el puntero mediante el método fill(), por lo que no fue necesario realizar la conversión. La principal recomendación aquí es crear menos métodos, para tener que hacer menos conversiones en cada llamada y no añadir demasiada sobrecarga.

Cabe señalar que el atributo `#[no_mangle]` y `extern "C"` son obligatorios para todos estos métodos. Sin ellos, no será posible realizar correctamente una compilación compatible con C/C++. Además, son necesarios para el siguiente paso de la integración.

Después de escribir el código para los métodos shim, debemos preparar el archivo de cabecera de la biblioteca. Esto puede hacerse manualmente, o puedes usar la biblioteca cbindgen para generarlo automáticamente. Si usas cbindgen, tendrás que escribir un script de compilación build.rs e incluir cbindgen como dependencia de compilación.

Un ejemplo de un script de compilación que puede generar automáticamente un archivo de cabecera:

```rust
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let package_name = env::var("CARGO_PKG_NAME").unwrap();
    let output_file = ("include/".to_owned() + &format!("{}.h", package_name)).to_string();

    match cbindgen::generate(&crate_dir) {
        Ok(header) => {
            header.write_to_file(&output_file);
        }
        Err(err) => {
            panic!("{}", err)
        }
    }
```

Además, debes usar el atributo #[no&#95;mangle] y `extern "C"` en cada atributo compatible con C. De lo contrario, la biblioteca puede compilarse incorrectamente y cbindgen no iniciará la generación automática de archivos de cabecera.

Después de todos estos pasos, puedes probar tu biblioteca en un proyecto pequeño para detectar cualquier problema de compatibilidad o de generación de cabeceras. Si surge algún problema durante la generación de cabeceras, puedes intentar configurarla con el archivo cbindgen.toml (puedes encontrar una plantilla aquí: [https://github.com/eqrion/cbindgen/blob/master/template.toml](https://github.com/eqrion/cbindgen/blob/master/template.toml)).

Cabe destacar el problema que surgió al integrar BLAKE3:
MemorySanitizer puede provocar falsos positivos, ya que no puede determinar si algunas variables en Rust están inicializadas o no. Se resolvió escribiendo un método con una definición más explícita para algunas variables, aunque esta implementación del método es más lenta y se usa únicamente para corregir las compilaciones de MemorySanitizer.