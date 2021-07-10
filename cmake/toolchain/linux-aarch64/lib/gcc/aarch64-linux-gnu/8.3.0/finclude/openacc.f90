!  OpenACC Runtime Library Definitions.

!  Copyright (C) 2014-2018 Free Software Foundation, Inc.

!  Contributed by Tobias Burnus <burnus@net-b.de>
!              and Mentor Embedded.

!  This file is part of the GNU Offloading and Multi Processing Library
!  (libgomp).

!  Libgomp is free software; you can redistribute it and/or modify it
!  under the terms of the GNU General Public License as published by
!  the Free Software Foundation; either version 3, or (at your option)
!  any later version.

!  Libgomp is distributed in the hope that it will be useful, but WITHOUT ANY
!  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
!  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for
!  more details.

!  Under Section 7 of GPL version 3, you are granted additional
!  permissions described in the GCC Runtime Library Exception, version
!  3.1, as published by the Free Software Foundation.

!  You should have received a copy of the GNU General Public License and
!  a copy of the GCC Runtime Library Exception along with this program;
!  see the files COPYING3 and COPYING.RUNTIME respectively.  If not, see
!  <http://www.gnu.org/licenses/>.

module openacc_kinds
  use iso_fortran_env, only: int32
  implicit none

  private :: int32
  public :: acc_device_kind

  integer, parameter :: acc_device_kind = int32

  public :: acc_device_none, acc_device_default, acc_device_host
  public :: acc_device_not_host, acc_device_nvidia

  ! Keep in sync with include/gomp-constants.h.
  integer (acc_device_kind), parameter :: acc_device_none = 0
  integer (acc_device_kind), parameter :: acc_device_default = 1
  integer (acc_device_kind), parameter :: acc_device_host = 2
  ! integer (acc_device_kind), parameter :: acc_device_host_nonshm = 3 removed.
  integer (acc_device_kind), parameter :: acc_device_not_host = 4
  integer (acc_device_kind), parameter :: acc_device_nvidia = 5

  public :: acc_handle_kind

  integer, parameter :: acc_handle_kind = int32

  public :: acc_async_noval, acc_async_sync

  ! Keep in sync with include/gomp-constants.h.
  integer (acc_handle_kind), parameter :: acc_async_noval = -1
  integer (acc_handle_kind), parameter :: acc_async_sync = -2

end module

module openacc_internal
  use openacc_kinds
  implicit none

  interface
    function acc_get_num_devices_h (d)
      import
      integer acc_get_num_devices_h
      integer (acc_device_kind) d
    end function

    subroutine acc_set_device_type_h (d)
      import
      integer (acc_device_kind) d
    end subroutine

    function acc_get_device_type_h ()
      import
      integer (acc_device_kind) acc_get_device_type_h
    end function

    subroutine acc_set_device_num_h (n, d)
      import
      integer n
      integer (acc_device_kind) d
    end subroutine

    function acc_get_device_num_h (d)
      import
      integer acc_get_device_num_h
      integer (acc_device_kind) d
    end function

    function acc_async_test_h (a)
      logical acc_async_test_h
      integer a
    end function

    function acc_async_test_all_h ()
      logical acc_async_test_all_h
    end function

    subroutine acc_wait_h (a)
      integer a
    end subroutine

    subroutine acc_wait_async_h (a1, a2)
      integer a1, a2
    end subroutine

    subroutine acc_wait_all_h ()
    end subroutine

    subroutine acc_wait_all_async_h (a)
      integer a
    end subroutine

    subroutine acc_init_h (d)
      import
      integer (acc_device_kind) d
    end subroutine

    subroutine acc_shutdown_h (d)
      import
      integer (acc_device_kind) d
    end subroutine

    function acc_on_device_h (d)
      import
      integer (acc_device_kind) d
      logical acc_on_device_h
    end function

    subroutine acc_copyin_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_copyin_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_copyin_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_present_or_copyin_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_present_or_copyin_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_present_or_copyin_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_create_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_create_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_create_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_present_or_create_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_present_or_create_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_present_or_create_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_copyout_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_copyout_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_copyout_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_delete_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_delete_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_delete_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_update_device_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_update_device_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_update_device_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    subroutine acc_update_self_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end subroutine

    subroutine acc_update_self_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end subroutine

    subroutine acc_update_self_array_h (a)
      type (*), dimension (..), contiguous :: a
    end subroutine

    function acc_is_present_32_h (a, len)
      use iso_c_binding, only: c_int32_t
      logical acc_is_present_32_h
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int32_t) len
    end function

    function acc_is_present_64_h (a, len)
      use iso_c_binding, only: c_int64_t
      logical acc_is_present_64_h
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_int64_t) len
    end function

    function acc_is_present_array_h (a)
      logical acc_is_present_array_h
      type (*), dimension (..), contiguous :: a
    end function
  end interface

  interface
    function acc_get_num_devices_l (d) &
        bind (C, name = "acc_get_num_devices")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_get_num_devices_l
      integer (c_int), value :: d
    end function

    subroutine acc_set_device_type_l (d) &
        bind (C, name = "acc_set_device_type")
      use iso_c_binding, only: c_int
      integer (c_int), value :: d
    end subroutine

    function acc_get_device_type_l () &
        bind (C, name = "acc_get_device_type")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_get_device_type_l
    end function

    subroutine acc_set_device_num_l (n, d) &
        bind (C, name = "acc_set_device_num")
      use iso_c_binding, only: c_int
      integer (c_int), value :: n, d
    end subroutine

    function acc_get_device_num_l (d) &
        bind (C, name = "acc_get_device_num")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_get_device_num_l
      integer (c_int), value :: d
    end function

    function acc_async_test_l (a) &
        bind (C, name = "acc_async_test")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_async_test_l
      integer (c_int), value :: a
    end function

    function acc_async_test_all_l () &
        bind (C, name = "acc_async_test_all")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_async_test_all_l
    end function

    subroutine acc_wait_l (a) &
        bind (C, name = "acc_wait")
      use iso_c_binding, only: c_int
      integer (c_int), value :: a
    end subroutine

    subroutine acc_wait_async_l (a1, a2) &
        bind (C, name = "acc_wait_async")
      use iso_c_binding, only: c_int
      integer (c_int), value :: a1, a2
    end subroutine

    subroutine acc_wait_all_l () &
        bind (C, name = "acc_wait_all")
      use iso_c_binding, only: c_int
    end subroutine

    subroutine acc_wait_all_async_l (a) &
        bind (C, name = "acc_wait_all_async")
      use iso_c_binding, only: c_int
      integer (c_int), value :: a
    end subroutine

    subroutine acc_init_l (d) &
        bind (C, name = "acc_init")
      use iso_c_binding, only: c_int
      integer (c_int), value :: d
    end subroutine

    subroutine acc_shutdown_l (d) &
        bind (C, name = "acc_shutdown")
      use iso_c_binding, only: c_int
      integer (c_int), value :: d
    end subroutine

    function acc_on_device_l (d) &
        bind (C, name = "acc_on_device")
      use iso_c_binding, only: c_int
      integer (c_int) :: acc_on_device_l
      integer (c_int), value :: d
    end function

    subroutine acc_copyin_l (a, len) &
        bind (C, name = "acc_copyin")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_present_or_copyin_l (a, len) &
        bind (C, name = "acc_present_or_copyin")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_create_l (a, len) &
        bind (C, name = "acc_create")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_present_or_create_l (a, len) &
        bind (C, name = "acc_present_or_create")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_copyout_l (a, len) &
        bind (C, name = "acc_copyout")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_delete_l (a, len) &
        bind (C, name = "acc_delete")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_update_device_l (a, len) &
        bind (C, name = "acc_update_device")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    subroutine acc_update_self_l (a, len) &
        bind (C, name = "acc_update_self")
      use iso_c_binding, only: c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end subroutine

    function acc_is_present_l (a, len) &
        bind (C, name = "acc_is_present")
      use iso_c_binding, only: c_int32_t, c_size_t
      !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
      integer (c_int32_t) :: acc_is_present_l
      type (*), dimension (*) :: a
      integer (c_size_t), value :: len
    end function
  end interface
end module

module openacc
  use openacc_kinds
  use openacc_internal
  implicit none

  public :: openacc_version

  public :: acc_get_num_devices, acc_set_device_type, acc_get_device_type
  public :: acc_set_device_num, acc_get_device_num, acc_async_test
  public :: acc_async_test_all
  public :: acc_wait, acc_async_wait, acc_wait_async
  public :: acc_wait_all, acc_async_wait_all, acc_wait_all_async
  public :: acc_init, acc_shutdown, acc_on_device
  public :: acc_copyin, acc_present_or_copyin, acc_pcopyin, acc_create
  public :: acc_present_or_create, acc_pcreate, acc_copyout, acc_delete
  public :: acc_update_device, acc_update_self, acc_is_present

  integer, parameter :: openacc_version = 201306

  interface acc_get_num_devices
    procedure :: acc_get_num_devices_h
  end interface

  interface acc_set_device_type
    procedure :: acc_set_device_type_h
  end interface

  interface acc_get_device_type
    procedure :: acc_get_device_type_h
  end interface

  interface acc_set_device_num
    procedure :: acc_set_device_num_h
  end interface

  interface acc_get_device_num
    procedure :: acc_get_device_num_h
  end interface

  interface acc_async_test
    procedure :: acc_async_test_h
  end interface

  interface acc_async_test_all
    procedure :: acc_async_test_all_h
  end interface

  interface acc_wait
    procedure :: acc_wait_h
  end interface

  ! acc_async_wait is an OpenACC 1.0 compatibility name for acc_wait.
  interface acc_async_wait
    procedure :: acc_wait_h
  end interface

  interface acc_wait_async
    procedure :: acc_wait_async_h
  end interface

  interface acc_wait_all
    procedure :: acc_wait_all_h
  end interface

  ! acc_async_wait_all is an OpenACC 1.0 compatibility name for acc_wait_all.
  interface acc_async_wait_all
    procedure :: acc_wait_all_h
  end interface

  interface acc_wait_all_async
    procedure :: acc_wait_all_async_h
  end interface

  interface acc_init
    procedure :: acc_init_h
  end interface

  interface acc_shutdown
    procedure :: acc_shutdown_h
  end interface

  interface acc_on_device
    procedure :: acc_on_device_h
  end interface

  ! acc_malloc: Only available in C/C++
  ! acc_free: Only available in C/C++

  ! As vendor extension, the following code supports both 32bit and 64bit
  ! arguments for "size"; the OpenACC standard only permits default-kind
  ! integers, which are of kind 4 (i.e. 32 bits).
  ! Additionally, the two-argument version also takes arrays as argument.
  ! and the one argument version also scalars. Note that the code assumes
  ! that the arrays are contiguous.

  interface acc_copyin
    procedure :: acc_copyin_32_h
    procedure :: acc_copyin_64_h
    procedure :: acc_copyin_array_h
  end interface

  interface acc_present_or_copyin
    procedure :: acc_present_or_copyin_32_h
    procedure :: acc_present_or_copyin_64_h
    procedure :: acc_present_or_copyin_array_h
  end interface

  interface acc_pcopyin
    procedure :: acc_present_or_copyin_32_h
    procedure :: acc_present_or_copyin_64_h
    procedure :: acc_present_or_copyin_array_h
  end interface

  interface acc_create
    procedure :: acc_create_32_h
    procedure :: acc_create_64_h
    procedure :: acc_create_array_h
  end interface

  interface acc_present_or_create
    procedure :: acc_present_or_create_32_h
    procedure :: acc_present_or_create_64_h
    procedure :: acc_present_or_create_array_h
  end interface

  interface acc_pcreate
    procedure :: acc_present_or_create_32_h
    procedure :: acc_present_or_create_64_h
    procedure :: acc_present_or_create_array_h
  end interface

  interface acc_copyout
    procedure :: acc_copyout_32_h
    procedure :: acc_copyout_64_h
    procedure :: acc_copyout_array_h
  end interface

  interface acc_delete
    procedure :: acc_delete_32_h
    procedure :: acc_delete_64_h
    procedure :: acc_delete_array_h
  end interface

  interface acc_update_device
    procedure :: acc_update_device_32_h
    procedure :: acc_update_device_64_h
    procedure :: acc_update_device_array_h
  end interface

  interface acc_update_self
    procedure :: acc_update_self_32_h
    procedure :: acc_update_self_64_h
    procedure :: acc_update_self_array_h
  end interface

  ! acc_map_data: Only available in C/C++
  ! acc_unmap_data: Only available in C/C++
  ! acc_deviceptr: Only available in C/C++
  ! acc_hostptr: Only available in C/C++

  interface acc_is_present
    procedure :: acc_is_present_32_h
    procedure :: acc_is_present_64_h
    procedure :: acc_is_present_array_h
  end interface

  ! acc_memcpy_to_device: Only available in C/C++
  ! acc_memcpy_from_device: Only available in C/C++

end module

function acc_get_num_devices_h (d)
  use openacc_internal, only: acc_get_num_devices_l
  use openacc_kinds
  integer acc_get_num_devices_h
  integer (acc_device_kind) d
  acc_get_num_devices_h = acc_get_num_devices_l (d)
end function

subroutine acc_set_device_type_h (d)
  use openacc_internal, only: acc_set_device_type_l
  use openacc_kinds
  integer (acc_device_kind) d
  call acc_set_device_type_l (d)
end subroutine

function acc_get_device_type_h ()
  use openacc_internal, only: acc_get_device_type_l
  use openacc_kinds
  integer (acc_device_kind) acc_get_device_type_h
  acc_get_device_type_h = acc_get_device_type_l ()
end function

subroutine acc_set_device_num_h (n, d)
  use openacc_internal, only: acc_set_device_num_l
  use openacc_kinds
  integer n
  integer (acc_device_kind) d
  call acc_set_device_num_l (n, d)
end subroutine

function acc_get_device_num_h (d)
  use openacc_internal, only: acc_get_device_num_l
  use openacc_kinds
  integer acc_get_device_num_h
  integer (acc_device_kind) d
  acc_get_device_num_h = acc_get_device_num_l (d)
end function

function acc_async_test_h (a)
  use openacc_internal, only: acc_async_test_l
  logical acc_async_test_h
  integer a
  if (acc_async_test_l (a) .eq. 1) then
    acc_async_test_h = .TRUE.
  else
    acc_async_test_h = .FALSE.
  end if
end function

function acc_async_test_all_h ()
  use openacc_internal, only: acc_async_test_all_l
  logical acc_async_test_all_h
  if (acc_async_test_all_l () .eq. 1) then
    acc_async_test_all_h = .TRUE.
  else
    acc_async_test_all_h = .FALSE.
  end if
end function

subroutine acc_wait_h (a)
  use openacc_internal, only: acc_wait_l
  integer a
  call acc_wait_l (a)
end subroutine

subroutine acc_wait_async_h (a1, a2)
  use openacc_internal, only: acc_wait_async_l
  integer a1, a2
  call acc_wait_async_l (a1, a2)
end subroutine

subroutine acc_wait_all_h ()
  use openacc_internal, only: acc_wait_all_l
  call acc_wait_all_l ()
end subroutine

subroutine acc_wait_all_async_h (a)
  use openacc_internal, only: acc_wait_all_async_l
  integer a
  call acc_wait_all_async_l (a)
end subroutine

subroutine acc_init_h (d)
  use openacc_internal, only: acc_init_l
  use openacc_kinds
  integer (acc_device_kind) d
  call acc_init_l (d)
end subroutine

subroutine acc_shutdown_h (d)
  use openacc_internal, only: acc_shutdown_l
  use openacc_kinds
  integer (acc_device_kind) d
  call acc_shutdown_l (d)
end subroutine

function acc_on_device_h (d)
  use openacc_internal, only: acc_on_device_l
  use openacc_kinds
  integer (acc_device_kind) d
  logical acc_on_device_h
  if (acc_on_device_l (d) .eq. 1) then
    acc_on_device_h = .TRUE.
  else
    acc_on_device_h = .FALSE.
  end if
end function

subroutine acc_copyin_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_copyin_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_copyin_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_copyin_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_copyin_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_copyin_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_copyin_array_h (a)
  use openacc_internal, only: acc_copyin_l
  type (*), dimension (..), contiguous :: a
  call acc_copyin_l (a, sizeof (a))
end subroutine

subroutine acc_present_or_copyin_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_present_or_copyin_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_present_or_copyin_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_present_or_copyin_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_present_or_copyin_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_present_or_copyin_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_present_or_copyin_array_h (a)
  use openacc_internal, only: acc_present_or_copyin_l
  type (*), dimension (..), contiguous :: a
  call acc_present_or_copyin_l (a, sizeof (a))
end subroutine

subroutine acc_create_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_create_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_create_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_create_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_create_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_create_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_create_array_h (a)
  use openacc_internal, only: acc_create_l
  type (*), dimension (..), contiguous :: a
  call acc_create_l (a, sizeof (a))
end subroutine

subroutine acc_present_or_create_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_present_or_create_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_present_or_create_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_present_or_create_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_present_or_create_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_present_or_create_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_present_or_create_array_h (a)
  use openacc_internal, only: acc_present_or_create_l
  type (*), dimension (..), contiguous :: a
  call acc_present_or_create_l (a, sizeof (a))
end subroutine

subroutine acc_copyout_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_copyout_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_copyout_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_copyout_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_copyout_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_copyout_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_copyout_array_h (a)
  use openacc_internal, only: acc_copyout_l
  type (*), dimension (..), contiguous :: a
  call acc_copyout_l (a, sizeof (a))
end subroutine

subroutine acc_delete_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_delete_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_delete_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_delete_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_delete_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_delete_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_delete_array_h (a)
  use openacc_internal, only: acc_delete_l
  type (*), dimension (..), contiguous :: a
  call acc_delete_l (a, sizeof (a))
end subroutine

subroutine acc_update_device_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_update_device_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_update_device_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_update_device_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_update_device_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_update_device_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_update_device_array_h (a)
  use openacc_internal, only: acc_update_device_l
  type (*), dimension (..), contiguous :: a
  call acc_update_device_l (a, sizeof (a))
end subroutine

subroutine acc_update_self_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_update_self_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  call acc_update_self_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_update_self_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_update_self_l
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  call acc_update_self_l (a, int (len, kind = c_size_t))
end subroutine

subroutine acc_update_self_array_h (a)
  use openacc_internal, only: acc_update_self_l
  type (*), dimension (..), contiguous :: a
  call acc_update_self_l (a, sizeof (a))
end subroutine

function acc_is_present_32_h (a, len)
  use iso_c_binding, only: c_int32_t, c_size_t
  use openacc_internal, only: acc_is_present_l
  logical acc_is_present_32_h
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int32_t) len
  if (acc_is_present_l (a, int (len, kind = c_size_t)) .eq. 1) then
    acc_is_present_32_h = .TRUE.
  else
    acc_is_present_32_h = .FALSE.
  end if
end function

function acc_is_present_64_h (a, len)
  use iso_c_binding, only: c_int64_t, c_size_t
  use openacc_internal, only: acc_is_present_l
  logical acc_is_present_64_h
  !GCC$ ATTRIBUTES NO_ARG_CHECK :: a
  type (*), dimension (*) :: a
  integer (c_int64_t) len
  if (acc_is_present_l (a, int (len, kind = c_size_t)) .eq. 1) then
    acc_is_present_64_h = .TRUE.
  else
    acc_is_present_64_h = .FALSE.
  end if
end function

function acc_is_present_array_h (a)
  use openacc_internal, only: acc_is_present_l
  logical acc_is_present_array_h
  type (*), dimension (..), contiguous :: a
  acc_is_present_array_h = acc_is_present_l (a, sizeof (a)) == 1
end function
