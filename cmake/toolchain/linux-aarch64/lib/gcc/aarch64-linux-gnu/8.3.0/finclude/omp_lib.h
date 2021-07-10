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

      integer omp_lock_kind, omp_nest_lock_kind, openmp_version
      parameter (omp_lock_kind = 4)
      parameter (omp_nest_lock_kind = 8)
      integer omp_sched_kind
      parameter (omp_sched_kind = 4)
      integer (omp_sched_kind) omp_sched_static, omp_sched_dynamic
      integer (omp_sched_kind) omp_sched_guided, omp_sched_auto
      parameter (omp_sched_static = 1)
      parameter (omp_sched_dynamic = 2)
      parameter (omp_sched_guided = 3)
      parameter (omp_sched_auto = 4)
      integer omp_proc_bind_kind
      parameter (omp_proc_bind_kind = 4)
      integer (omp_proc_bind_kind) omp_proc_bind_false
      integer (omp_proc_bind_kind) omp_proc_bind_true
      integer (omp_proc_bind_kind) omp_proc_bind_master
      integer (omp_proc_bind_kind) omp_proc_bind_close
      integer (omp_proc_bind_kind) omp_proc_bind_spread
      parameter (omp_proc_bind_false = 0)
      parameter (omp_proc_bind_true = 1)
      parameter (omp_proc_bind_master = 2)
      parameter (omp_proc_bind_close = 3)
      parameter (omp_proc_bind_spread = 4)
      integer omp_lock_hint_kind
      parameter (omp_lock_hint_kind = 4)
      integer (omp_lock_hint_kind) omp_lock_hint_none
      integer (omp_lock_hint_kind) omp_lock_hint_uncontended
      integer (omp_lock_hint_kind) omp_lock_hint_contended
      integer (omp_lock_hint_kind) omp_lock_hint_nonspeculative
      integer (omp_lock_hint_kind) omp_lock_hint_speculative
      parameter (omp_lock_hint_none = 0)
      parameter (omp_lock_hint_uncontended = 1)
      parameter (omp_lock_hint_contended = 2)
      parameter (omp_lock_hint_nonspeculative = 4)
      parameter (omp_lock_hint_speculative = 8)
      parameter (openmp_version = 201511)

      external omp_init_lock, omp_init_nest_lock
      external omp_init_lock_with_hint
      external omp_init_nest_lock_with_hint
      external omp_destroy_lock, omp_destroy_nest_lock
      external omp_set_lock, omp_set_nest_lock
      external omp_unset_lock, omp_unset_nest_lock
      external omp_set_dynamic, omp_set_nested
      external omp_set_num_threads

      external omp_get_dynamic, omp_get_nested
      logical(4) omp_get_dynamic, omp_get_nested
      external omp_test_lock, omp_in_parallel
      logical(4) omp_test_lock, omp_in_parallel

      external omp_get_max_threads, omp_get_num_procs
      integer(4) omp_get_max_threads, omp_get_num_procs
      external omp_get_num_threads, omp_get_thread_num
      integer(4) omp_get_num_threads, omp_get_thread_num
      external omp_test_nest_lock
      integer(4) omp_test_nest_lock

      external omp_get_wtick, omp_get_wtime
      double precision omp_get_wtick, omp_get_wtime

      external omp_set_schedule, omp_get_schedule
      external omp_get_thread_limit, omp_set_max_active_levels
      external omp_get_max_active_levels, omp_get_level
      external omp_get_ancestor_thread_num, omp_get_team_size
      external omp_get_active_level
      integer(4) omp_get_thread_limit, omp_get_max_active_levels
      integer(4) omp_get_level, omp_get_ancestor_thread_num
      integer(4) omp_get_team_size, omp_get_active_level

      external omp_in_final
      logical(4) omp_in_final

      external omp_get_cancelllation
      logical(4) omp_get_cancelllation

      external omp_get_proc_bind
      integer(omp_proc_bind_kind) omp_get_proc_bind

      integer(4) omp_get_num_places
      external omp_get_num_places
      integer(4) omp_get_place_num_procs
      external omp_get_place_num_procs
      external omp_get_place_proc_ids
      integer(4) omp_get_place_num
      external omp_get_place_num
      integer(4) omp_get_partition_num_places
      external omp_get_partition_num_places
      external omp_get_partition_place_nums

      external omp_set_default_device, omp_get_default_device
      external omp_get_num_devices, omp_get_num_teams
      external omp_get_team_num
      integer(4) omp_get_default_device, omp_get_num_devices
      integer(4) omp_get_num_teams, omp_get_team_num

      external omp_is_initial_device
      logical(4) omp_is_initial_device
      external omp_get_initial_device
      integer(4) omp_get_initial_device

      external omp_get_max_task_priority
      integer(4) omp_get_max_task_priority
