!  Copyright (C) 2005-2018 Free Software Foundation, Inc.
!  Contributed by Jakub Jelinek <jakub@redhat.com>.

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

      module omp_lib_kinds
        implicit none
        integer, parameter :: omp_lock_kind = 4
        integer, parameter :: omp_nest_lock_kind = 8
        integer, parameter :: omp_sched_kind = 4
        integer, parameter :: omp_proc_bind_kind = 4
        integer, parameter :: omp_lock_hint_kind = 4
        integer (omp_sched_kind), parameter :: omp_sched_static = 1
        integer (omp_sched_kind), parameter :: omp_sched_dynamic = 2
        integer (omp_sched_kind), parameter :: omp_sched_guided = 3
        integer (omp_sched_kind), parameter :: omp_sched_auto = 4
        integer (omp_proc_bind_kind), &
                 parameter :: omp_proc_bind_false = 0
        integer (omp_proc_bind_kind), &
                 parameter :: omp_proc_bind_true = 1
        integer (omp_proc_bind_kind), &
                 parameter :: omp_proc_bind_master = 2
        integer (omp_proc_bind_kind), &
                 parameter :: omp_proc_bind_close = 3
        integer (omp_proc_bind_kind), &
                 parameter :: omp_proc_bind_spread = 4
        integer (omp_lock_hint_kind), &
                 parameter :: omp_lock_hint_none = 0
        integer (omp_lock_hint_kind), &
                 parameter :: omp_lock_hint_uncontended = 1
        integer (omp_lock_hint_kind), &
                 parameter :: omp_lock_hint_contended = 2
        integer (omp_lock_hint_kind), &
                 parameter :: omp_lock_hint_nonspeculative = 4
        integer (omp_lock_hint_kind), &
                 parameter :: omp_lock_hint_speculative = 8
      end module

      module omp_lib
        use omp_lib_kinds
        implicit none
        integer, parameter :: openmp_version = 201511

        interface
          subroutine omp_init_lock (svar)
            use omp_lib_kinds
            integer (omp_lock_kind), intent (out) :: svar
          end subroutine omp_init_lock
        end interface

        interface
          subroutine omp_init_lock_with_hint (svar, hint)
            use omp_lib_kinds
            integer (omp_lock_kind), intent (out) :: svar
            integer (omp_lock_hint_kind), intent (in) :: hint
          end subroutine omp_init_lock_with_hint
        end interface

        interface
          subroutine omp_init_nest_lock (nvar)
            use omp_lib_kinds
            integer (omp_nest_lock_kind), intent (out) :: nvar
          end subroutine omp_init_nest_lock
        end interface

        interface
          subroutine omp_init_nest_lock_with_hint (nvar, hint)
            use omp_lib_kinds
            integer (omp_nest_lock_kind), intent (out) :: nvar
            integer (omp_lock_hint_kind), intent (in) :: hint
          end subroutine omp_init_nest_lock_with_hint
        end interface

        interface
          subroutine omp_destroy_lock (svar)
            use omp_lib_kinds
            integer (omp_lock_kind), intent (inout) :: svar
          end subroutine omp_destroy_lock
        end interface

        interface
          subroutine omp_destroy_nest_lock (nvar)
            use omp_lib_kinds
            integer (omp_nest_lock_kind), intent (inout) :: nvar
          end subroutine omp_destroy_nest_lock
        end interface

        interface
          subroutine omp_set_lock (svar)
            use omp_lib_kinds
            integer (omp_lock_kind), intent (inout) :: svar
          end subroutine omp_set_lock
        end interface

        interface
          subroutine omp_set_nest_lock (nvar)
            use omp_lib_kinds
            integer (omp_nest_lock_kind), intent (inout) :: nvar
          end subroutine omp_set_nest_lock
        end interface

        interface
          subroutine omp_unset_lock (svar)
            use omp_lib_kinds
            integer (omp_lock_kind), intent (inout) :: svar
          end subroutine omp_unset_lock
        end interface

        interface
          subroutine omp_unset_nest_lock (nvar)
            use omp_lib_kinds
            integer (omp_nest_lock_kind), intent (inout) :: nvar
          end subroutine omp_unset_nest_lock
        end interface

        interface omp_set_dynamic
          subroutine omp_set_dynamic (dynamic_threads)
            logical (4), intent (in) :: dynamic_threads
          end subroutine omp_set_dynamic
          subroutine omp_set_dynamic_8 (dynamic_threads)
            logical (8), intent (in) :: dynamic_threads
          end subroutine omp_set_dynamic_8
        end interface

        interface omp_set_nested
          subroutine omp_set_nested (nested)
            logical (4), intent (in) :: nested
          end subroutine omp_set_nested
          subroutine omp_set_nested_8 (nested)
            logical (8), intent (in) :: nested
          end subroutine omp_set_nested_8
        end interface

        interface omp_set_num_threads
          subroutine omp_set_num_threads (num_threads)
            integer (4), intent (in) :: num_threads
          end subroutine omp_set_num_threads
          subroutine omp_set_num_threads_8 (num_threads)
            integer (8), intent (in) :: num_threads
          end subroutine omp_set_num_threads_8
        end interface

        interface
          function omp_get_dynamic ()
            logical (4) :: omp_get_dynamic
          end function omp_get_dynamic
        end interface

        interface
          function omp_get_nested ()
            logical (4) :: omp_get_nested
          end function omp_get_nested
        end interface

        interface
          function omp_in_parallel ()
            logical (4) :: omp_in_parallel
          end function omp_in_parallel
        end interface

        interface
          function omp_test_lock (svar)
            use omp_lib_kinds
            logical (4) :: omp_test_lock
            integer (omp_lock_kind), intent (inout) :: svar
          end function omp_test_lock
        end interface

        interface
          function omp_get_max_threads ()
            integer (4) :: omp_get_max_threads
          end function omp_get_max_threads
        end interface

        interface
          function omp_get_num_procs ()
            integer (4) :: omp_get_num_procs
          end function omp_get_num_procs
        end interface

        interface
          function omp_get_num_threads ()
            integer (4) :: omp_get_num_threads
          end function omp_get_num_threads
        end interface

        interface
          function omp_get_thread_num ()
            integer (4) :: omp_get_thread_num
          end function omp_get_thread_num
        end interface

        interface
          function omp_test_nest_lock (nvar)
            use omp_lib_kinds
            integer (4) :: omp_test_nest_lock
            integer (omp_nest_lock_kind), intent (inout) :: nvar
          end function omp_test_nest_lock
        end interface

        interface
          function omp_get_wtick ()
            double precision :: omp_get_wtick
          end function omp_get_wtick
        end interface

        interface
          function omp_get_wtime ()
            double precision :: omp_get_wtime
          end function omp_get_wtime
        end interface

        interface omp_set_schedule
          subroutine omp_set_schedule (kind, chunk_size)
            use omp_lib_kinds
            integer (omp_sched_kind), intent (in) :: kind
            integer (4), intent (in) :: chunk_size
          end subroutine omp_set_schedule
          subroutine omp_set_schedule_8 (kind, chunk_size)
            use omp_lib_kinds
            integer (omp_sched_kind), intent (in) :: kind
            integer (8), intent (in) :: chunk_size
          end subroutine omp_set_schedule_8
         end interface

        interface omp_get_schedule
          subroutine omp_get_schedule (kind, chunk_size)
            use omp_lib_kinds
            integer (omp_sched_kind), intent (out) :: kind
            integer (4), intent (out) :: chunk_size
          end subroutine omp_get_schedule
          subroutine omp_get_schedule_8 (kind, chunk_size)
            use omp_lib_kinds
            integer (omp_sched_kind), intent (out) :: kind
            integer (8), intent (out) :: chunk_size
          end subroutine omp_get_schedule_8
         end interface

        interface
          function omp_get_thread_limit ()
            integer (4) :: omp_get_thread_limit
          end function omp_get_thread_limit
        end interface

        interface omp_set_max_active_levels
          subroutine omp_set_max_active_levels (max_levels)
            integer (4), intent (in) :: max_levels
          end subroutine omp_set_max_active_levels
          subroutine omp_set_max_active_levels_8 (max_levels)
            integer (8), intent (in) :: max_levels
          end subroutine omp_set_max_active_levels_8
        end interface

        interface
          function omp_get_max_active_levels ()
            integer (4) :: omp_get_max_active_levels
          end function omp_get_max_active_levels
        end interface

        interface
          function omp_get_level ()
            integer (4) :: omp_get_level
          end function omp_get_level
        end interface

        interface omp_get_ancestor_thread_num
          function omp_get_ancestor_thread_num (level)
            integer (4), intent (in) :: level
            integer (4) :: omp_get_ancestor_thread_num
          end function omp_get_ancestor_thread_num
          function omp_get_ancestor_thread_num_8 (level)
            integer (8), intent (in) :: level
            integer (4) :: omp_get_ancestor_thread_num_8
          end function omp_get_ancestor_thread_num_8
        end interface

        interface omp_get_team_size
          function omp_get_team_size (level)
            integer (4), intent (in) :: level
            integer (4) :: omp_get_team_size
          end function omp_get_team_size
          function omp_get_team_size_8 (level)
            integer (8), intent (in) :: level
            integer (4) :: omp_get_team_size_8
          end function omp_get_team_size_8
        end interface

        interface
          function omp_get_active_level ()
            integer (4) :: omp_get_active_level
          end function omp_get_active_level
        end interface

        interface
          function omp_in_final ()
            logical (4) :: omp_in_final
          end function omp_in_final
        end interface

        interface
          function omp_get_cancellation ()
            logical (4) :: omp_get_cancellation
          end function omp_get_cancellation
        end interface

        interface
          function omp_get_proc_bind ()
            use omp_lib_kinds
            integer (omp_proc_bind_kind) :: omp_get_proc_bind
          end function omp_get_proc_bind
        end interface

        interface
          function omp_get_num_places ()
            integer (4) :: omp_get_num_places
          end function omp_get_num_places
        end interface

        interface omp_get_place_num_procs
          function omp_get_place_num_procs (place_num)
            integer (4), intent(in) :: place_num
            integer (4) :: omp_get_place_num_procs
          end function omp_get_place_num_procs

          function omp_get_place_num_procs_8 (place_num)
            integer (8), intent(in) :: place_num
            integer (4) :: omp_get_place_num_procs_8
          end function omp_get_place_num_procs_8
        end interface

        interface omp_get_place_proc_ids
          subroutine omp_get_place_proc_ids (place_num, ids)
            integer (4), intent(in) :: place_num
            integer (4), intent(out) :: ids(*)
          end subroutine omp_get_place_proc_ids

          subroutine omp_get_place_proc_ids_8 (place_num, ids)
            integer (8), intent(in) :: place_num
            integer (8), intent(out) :: ids(*)
          end subroutine omp_get_place_proc_ids_8
        end interface

        interface
          function omp_get_place_num ()
            integer (4) :: omp_get_place_num
          end function omp_get_place_num
        end interface

        interface
          function omp_get_partition_num_places ()
            integer (4) :: omp_get_partition_num_places
          end function omp_get_partition_num_places
        end interface

        interface omp_get_partition_place_nums
          subroutine omp_get_partition_place_nums (place_nums)
            integer (4), intent(out) :: place_nums(*)
          end subroutine omp_get_partition_place_nums

          subroutine omp_get_partition_place_nums_8 (place_nums)
            integer (8), intent(out) :: place_nums(*)
          end subroutine omp_get_partition_place_nums_8
        end interface

        interface omp_set_default_device
          subroutine omp_set_default_device (device_num)
            integer (4), intent (in) :: device_num
          end subroutine omp_set_default_device
          subroutine omp_set_default_device_8 (device_num)
            integer (8), intent (in) :: device_num
          end subroutine omp_set_default_device_8
        end interface

        interface
          function omp_get_default_device ()
            integer (4) :: omp_get_default_device
          end function omp_get_default_device
        end interface

        interface
          function omp_get_num_devices ()
            integer (4) :: omp_get_num_devices
          end function omp_get_num_devices
        end interface

        interface
          function omp_get_num_teams ()
            integer (4) :: omp_get_num_teams
          end function omp_get_num_teams
        end interface

        interface
          function omp_get_team_num ()
            integer (4) :: omp_get_team_num
          end function omp_get_team_num
        end interface

        interface
          function omp_is_initial_device ()
            logical (4) :: omp_is_initial_device
          end function omp_is_initial_device
        end interface

        interface
          function omp_get_initial_device ()
            integer (4) :: omp_get_initial_device
          end function omp_get_initial_device
        end interface

        interface
          function omp_get_max_task_priority ()
            integer (4) :: omp_get_max_task_priority
          end function omp_get_max_task_priority
        end interface

      end module omp_lib
