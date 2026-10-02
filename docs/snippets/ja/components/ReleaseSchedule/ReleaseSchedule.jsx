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

  return (
    <table>
      <thead>
        <tr>
          <th rowSpan={2}>バージョン</th>
          <th colSpan={2}>
            <a href="/docs/manage/updates#fast-release-channel-early-upgrades">Fast チャネル</a>
          </th>
          <th colSpan={2}>
            <a href="/docs/manage/updates#regular-release-channel">Regular チャネル</a>
          </th>
          <th colSpan={2}>
            <a href="/docs/manage/updates#slow-release-channel-deferred-upgrades">Slow チャネル</a>
          </th>
        </tr>
        <tr>
          <th>ロールアウト開始</th>
          <th>ロールアウト完了</th>
          <th>ロールアウト開始</th>
          <th>ロールアウト完了</th>
          <th>ロールアウト開始</th>
          <th>ロールアウト完了</th>
        </tr>
      </thead>
      <tbody>
        {releases.map((release, idx) => (
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
            <td><DateCell date={release.fast_start_date} note={release.fast_delay_note} status={release.fast_progress} /></td>
            <td><DateCell date={release.fast_end_date} status={release.fast_progress} /></td>
            <td><DateCell date={release.regular_start_date} note={release.regular_delay_note} status={release.regular_progress} /></td>
            <td><DateCell date={release.regular_end_date} status={release.regular_progress} /></td>
            <td><DateCell date={release.slow_start_date} note={release.slow_delay_note} status={release.slow_progress} /></td>
            <td><DateCell date={release.slow_end_date} status={release.slow_progress} /></td>
          </tr>
        ))}
      </tbody>
    </table>
  );
};
export default ReleaseSchedule;