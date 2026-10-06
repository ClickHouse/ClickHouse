const ReleaseSchedule = ({ releases = [] }) => {
  const StatusIndicator = ({ status }) => {
    const color =
      status === "green" ? "#22c55e" :
      status === "orange" ? "#f59e0b" :
      "#ef4444";
    return (
      <span style={{
        display: "inline-block",
        width: 8,
        height: 8,
        borderRadius: "50%",
        background: color,
        marginRight: 6,
      }} />
    );
  };

  const DateCell = ({ date, note, status }) => (
    <span>
      <StatusIndicator status={status} />
      {date}
      {note && <Tooltip tip={note}><Icon icon="circle-info" size={12} /></Tooltip>}
    </span>
  );

  const ChannelSchedule = ({ startDate, endDate, note, status }) => (
    <span style={{ display: "flex", flexDirection: "column", gap: 4 }}>
      <span>
        <strong>Inicio:</strong>{" "}
        <DateCell date={startDate} note={note} status={status} />
      </span>
      <span>
        <strong>Fin:</strong>{" "}
        <DateCell date={endDate} status={status} />
      </span>
    </span>
  );

  return (
    <table>
      <thead>
        <tr>
          <th>Versión</th>
          <th>
            <a href="/docs/manage/updates#fast-release-channel-early-upgrades">Canal rápido</a>
          </th>
          <th>
            <a href="/docs/manage/updates#regular-release-channel">Canal regular</a>
          </th>
          <th>
            <a href="/docs/manage/updates#slow-release-channel-deferred-upgrades">Canal lento</a>
          </th>
        </tr>
      </thead>
      <tbody>
        {releases.map((release, idx) => {
          const isCompleted = [
            release.fast_start_date,
            release.fast_end_date,
            release.regular_start_date,
            release.regular_end_date,
            release.slow_start_date,
            release.slow_end_date,
          ].every((date) => date === "Completado");

          return (
            <tr key={idx}>
              <td>
                {release.changelog_link ? (
                  <a href={release.changelog_link} target="_blank" rel="noopener noreferrer">
                    {release.version}
                  </a>
                ) : (
                  release.version
                )}
              </td>
              {isCompleted ? (
                <td colSpan={3} style={{ textAlign: "center" }}>
                  <DateCell date="Completado" status="green" />
                </td>
              ) : (
                <>
                  <td>
                    <ChannelSchedule
                      startDate={release.fast_start_date}
                      endDate={release.fast_end_date}
                      note={release.fast_delay_note}
                      status={release.fast_progress}
                    />
                  </td>
                  <td>
                    <ChannelSchedule
                      startDate={release.regular_start_date}
                      endDate={release.regular_end_date}
                      note={release.regular_delay_note}
                      status={release.regular_progress}
                    />
                  </td>
                  <td>
                    <ChannelSchedule
                      startDate={release.slow_start_date}
                      endDate={release.slow_end_date}
                      note={release.slow_delay_note}
                      status={release.slow_progress}
                    />
                  </td>
                </>
              )}
            </tr>
          );
        })}
      </tbody>
    </table>
  );
};
export default ReleaseSchedule;